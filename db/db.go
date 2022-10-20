package db

import (
	"context"
	"fmt"

	"github.com/cyverse-de/go-mod/logging"
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exec"
	"github.com/sirupsen/logrus"

	"github.com/jmoiron/sqlx"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "db"})

func New(dbconn *sqlx.DB) *goqu.Database {
	return goqu.New("postgresql", dbconn)
}

type QuerySettings struct {
	hasLimit  bool
	limit     uint
	hasOffset bool
	offset    uint
}

type QueryOption func(*QuerySettings)

func WithQueryLimit(limit uint) QueryOption {
	return func(s *QuerySettings) {
		s.hasLimit = true
		s.limit = limit
	}
}

func WithQueryOffset(offset uint) QueryOption {
	return func(s *QuerySettings) {
		s.hasOffset = true
		s.offset = offset
	}
}

func UserUpdates(ctx context.Context, dbconn *goqu.Database, username string, opts ...QueryOption) ([]Update, error) {
	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	opsT := goqu.T("update_operations")
	updatesT := goqu.T("updates")
	usersT := goqu.T("users")
	rtT := goqu.T("resource_types")

	query := goqu.From(updatesT).
		Join(usersT, goqu.On(goqu.I("updates.user_id").Eq(goqu.I("users.id")))).
		Join(opsT, goqu.On(goqu.I("updates.update_operation_id").Eq(goqu.I("update_operations.id")))).
		Join(rtT, goqu.On(goqu.I("updates.resource_type_id").Eq(goqu.I("resource_types.id")))).
		Where(goqu.Ex{
			"users.username": username,
		})

	if querySettings.hasLimit {
		query = query.Limit(querySettings.limit)
	}

	if querySettings.hasOffset {
		query = query.Offset(querySettings.offset)
	}

	qs, _, err := query.ToSQL()
	if err != nil {
		return nil, err
	}

	results := make([]Update, 0)
	if err = dbconn.ScanStructsContext(ctx, &results, qs); err != nil {
		return nil, err
	}

	return results, nil
}

func GetResourceTypeID(ctx context.Context, dbconn *goqu.Database, name, unit string) (string, error) {
	rtT := goqu.T("resource_types")
	query := goqu.From(rtT).
		Select("id").
		Where(goqu.Ex{
			"name": name,
			"unit": unit,
		})
	qs, _, err := query.ToSQL()
	if err != nil {
		return "", err
	}
	var result string
	if _, err = dbconn.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

func GetOperationID(ctx context.Context, dbconn *goqu.Database, name string) (string, error) {
	opT := goqu.T("update_operations")
	query := goqu.From(opT).
		Select("id").
		Where(goqu.Ex{
			"name": name,
		})
	qs, _, err := query.ToSQL()
	if err != nil {
		return "", err
	}
	var result string
	if _, err = dbconn.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

func GetUserID(ctx context.Context, dbconn *goqu.Database, username string) (string, error) {
	usersT := goqu.T("users")
	query := goqu.From(usersT).
		Select("id").
		Where(goqu.Ex{
			"username": username,
		})
	qs, _, err := query.ToSQL()
	if err != nil {
		return "", err
	}
	var result string
	if _, err = dbconn.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

func GetActiveUserPlan(ctx context.Context, db *goqu.Database, username string) (*UserPlan, error) {
	var (
		err    error
		result UserPlan
	)

	userPlansT := goqu.T("user_plans")
	usersT := goqu.T("users")
	effStartDate := goqu.I("user_plans.effective_start_date")
	effEndDate := goqu.I("user_plans.effective_end_date")
	currTS := goqu.L("CURRENT_TIMESTAMP")

	query := db.From(userPlansT).
		Select(
			userPlansT.Col("id"),
			"user_id",
			"plan_id",
			"effective_start_date",
			"effective_end_date",
			"created_by",
			"created_at",
			"last_modified_by",
			"last_modified_at",
		).
		Join(usersT, goqu.On(goqu.I("user_plans.user_id").Eq(goqu.I("users.id")))).
		Where(goqu.And(
			goqu.I("users.username").Eq(username),
			goqu.Or(
				currTS.Between(goqu.Range(effStartDate, effEndDate)),
				goqu.And(currTS.Gt(effStartDate), effEndDate.Is(nil)),
			),
		)).
		Order(effStartDate.Desc()).
		Limit(1).
		Executor()

	log.Debug(query.ToSQL())

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func AddUserUpdate(ctx context.Context, db *goqu.Database, update *Update) (*Update, error) {
	ds := db.Insert("updates").Rows(
		goqu.Record{
			"value_type":          update.ValueType,
			"value":               update.Value,
			"effective_date":      update.EffectiveDate,
			"update_operation_id": update.UpdateOperation.ID,
			"resource_type_id":    update.ResourceType.ID,
			"user_id":             update.User.ID,
		},
	).
		Returning(goqu.C("id")).
		Executor()

	var id string

	if _, err := ds.ScanValContext(ctx, &id); err != nil {
		return nil, err
	}

	update.ID = id

	return update, nil
}

func ProcessUpdateForUsage(ctx context.Context, db *goqu.Database, update *Update) error {
	log = log.WithFields(logrus.Fields{"context": "usage update", "user": update.User.Username})

	log.Debug("before getting active user plan")
	// Look up the currently active user plan, adding a default plan if one doesn't exist already.
	userPlan, err := GetActiveUserPlan(ctx, db, update.User.Username)
	if err != nil {
		return err
	}
	log.Debugf("after getting active user plan %s", userPlan.ID)

	log.Debug("beginning transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	log.Debug("after beginning transaction")

	if err = tx.Wrap(func() error {
		log.Debug("getting current usage")
		usagesE := tx.From("usages").
			Select(goqu.C("usage")).
			Where(goqu.And(
				goqu.I("resource_type_id").Eq(update.ResourceType.ID),
				goqu.I("user_plan_id").Eq(userPlan.ID),
			)).
			Limit(1).
			Executor()

		var usageValue float64
		usageFound, err := usagesE.ScanValContext(ctx, &usageValue)
		if err != nil {
			return err
		}
		log.Debugf("done getting current usage of %f", usageValue)

		switch update.UpdateOperation.Name {
		case UpdateTypeSet:
			usageValue = update.Value
		case UpdateTypeAdd:
			usageValue = usageValue + update.Value
		default:
			return fmt.Errorf("invalid update type: %s", update.UpdateOperation.Name)
		}
		log.Debugf("new usage value is %f", usageValue)

		log.Debug("upserting new usage value")
		updateRecord := goqu.Record{
			"usage":            usageValue,
			"resource_type_id": update.ResourceType.ID,
			"user_plan_id":     userPlan.ID,
			"last_modified_by": "de",
			"created_by":       "de",
		}

		var upsertE exec.QueryExecutor
		if !usageFound {
			upsertE = tx.Insert("usages").Rows(updateRecord).Executor()
		} else {
			upsertE = tx.Update("usages").Set(updateRecord).Where(
				goqu.And(
					goqu.I("resource_type_id").Eq(update.ResourceType.ID),
					goqu.I("user_plan_id").Eq(userPlan.ID),
				),
			).Executor()
		}

		log.Info(upsertE.ToSQL())

		_, err = upsertE.ExecContext(ctx)
		if err != nil {
			return err
		}
		log.Debug("done upserting new value")

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func ProcessUpdateForQuota(ctx context.Context, db *goqu.Database, update *Update) error {
	var (
		err        error
		quotaValue float64
	)

	userPlan, err := GetActiveUserPlan(ctx, db, update.User.Username)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err = tx.Wrap(func() error {
		quotasE := tx.From("quotas").
			Select(goqu.C("quota")).
			Where(goqu.And(
				goqu.I("resource_type_id").Eq(update.ResourceType.ID),
				goqu.I("user_plan_id").Eq(userPlan.ID),
			)).
			Limit(1).
			Executor()

		if _, err := quotasE.ScanValContext(ctx, &quotaValue); err != nil {
			return err
		}

		switch update.UpdateOperation.Name {
		case UpdateTypeSet:
			quotaValue = update.Value
		case UpdateTypeAdd:
			quotaValue = quotaValue + update.Value
		default:
			return fmt.Errorf("invalid update type: %s", update.UpdateOperation.Name)
		}

		upsertE := tx.Insert("quotas").Rows(
			goqu.Record{},
		).
			OnConflict(goqu.DoUpdate("resource_type_id", goqu.C("quota").Set(quotaValue))).
			OnConflict(goqu.DoUpdate("user_plan_id", goqu.C("quota").Set(quotaValue))).
			Executor()

		_, err = upsertE.ExecContext(ctx)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
