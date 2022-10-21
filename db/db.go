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

type Database struct {
	db     *sqlx.DB
	fullDB *goqu.Database
	goquDB GoquDatabase
}

func New(dbconn *sqlx.DB) *Database {
	goquDB := goqu.New("postgresql", dbconn)
	return &Database{
		db:     dbconn, // Used when a method needs direct access to sqlx for struct scanning.
		fullDB: goquDB, // Used when a method needs to use a method not defined in the GoquDatabase interface.
		goquDB: goquDB, // Used when a method needs to optionally support being run inside a transaction.

	}
}

type QuerySettings struct {
	hasLimit  bool
	limit     uint
	hasOffset bool
	offset    uint
	tx        *goqu.TxDatabase
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

func WithTX(tx *goqu.TxDatabase) QueryOption {
	return func(s *QuerySettings) {
		s.tx = tx
	}
}

func (d *Database) UserUpdates(ctx context.Context, username string, opts ...QueryOption) ([]Update, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	opsT := goqu.T("update_operations")
	updatesT := goqu.T("updates")
	usersT := goqu.T("users")
	rtT := goqu.T("resource_types")

	query := db.From(updatesT).
		Select(
			updatesT.Col("id"),
			updatesT.Col("user_id"),
			updatesT.Col("value_type"),
			updatesT.Col("value"),
			updatesT.Col("update_operation_id"),
			updatesT.Col("effective_date"),
			updatesT.Col("created_by"),
			updatesT.Col("created_at"),
			updatesT.Col("last_modified_by"),
			updatesT.Col("last_modified_at"),
			rtT.Col("id").As("resource_type_id"),
			rtT.Col("name").As("resource_type_name"),
			rtT.Col("unit").As("resource_type_unit"),
			usersT.Col("username"),
			opsT.Col("name").As("update_operation_name"),
		).
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

	var results []Update
	rows, err := d.db.QueryxContext(ctx, qs)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var u Update
		if err = rows.StructScan(&u); err != nil {
			return nil, err
		}
		results = append(results, u)
	}

	err = rows.Err()
	if err != nil {
		return results, err
	}

	return results, nil
}

func (d *Database) GetResourceTypeID(ctx context.Context, name, unit string, opts ...QueryOption) (string, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	rtT := goqu.T("resource_types")
	query := db.From(rtT).
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
	if _, err = db.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

func (d *Database) GetResourceType(ctx context.Context, id string, opts ...QueryOption) (*ResourceType, error) {
	var (
		err    error
		db     GoquDatabase
		result ResourceType
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	rtT := goqu.T("resource_types")
	query := db.From(rtT).
		Select(
			rtT.Col("id"),
			rtT.Col("name"),
			rtT.Col("unit"),
		).
		Where(rtT.Col("id").Eq(id)).
		Executor()

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}

	return &result, err
}

func (d *Database) GetOperationID(ctx context.Context, name string, opts ...QueryOption) (string, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	opT := goqu.T("update_operations")
	query := db.From(opT).
		Select("id").
		Where(goqu.Ex{
			"name": name,
		})
	qs, _, err := query.ToSQL()
	if err != nil {
		return "", err
	}
	var result string
	if _, err = db.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

func (d *Database) GetOperation(ctx context.Context, id string, opts ...QueryOption) (*UpdateOperation, error) {
	var (
		err    error
		db     GoquDatabase
		result UpdateOperation
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	opT := goqu.T("update_operations")
	query := db.From(opT).
		Select("id", "name").
		Where(goqu.Ex{
			"id": id,
		}).
		Executor()

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}

	return &result, err
}

func (d *Database) GetUserID(ctx context.Context, username string, opts ...QueryOption) (string, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	usersT := goqu.T("users")
	query := db.From(usersT).
		Select("id").
		Where(goqu.Ex{
			"username": username,
		})
	qs, _, err := query.ToSQL()
	if err != nil {
		return "", err
	}
	var result string
	if _, err = db.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

func (d *Database) GetUser(ctx context.Context, id string, opts ...QueryOption) (*User, error) {
	var (
		err    error
		db     GoquDatabase
		result User
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	usersT := goqu.T("users")
	query := db.From(usersT).
		Select("id", "username").
		Where(goqu.Ex{
			"id": id,
		}).
		Executor()

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (d *Database) GetActiveUserPlan(ctx context.Context, username string, opts ...QueryOption) (*UserPlan, error) {
	var (
		err    error
		result UserPlan
		db     GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	userPlansT := goqu.T("user_plans")
	usersT := goqu.T("users")
	effStartDate := goqu.I("user_plans.effective_start_date")
	effEndDate := goqu.I("user_plans.effective_end_date")
	currTS := goqu.L("CURRENT_TIMESTAMP")

	query := db.From(userPlansT).
		Select(
			userPlansT.Col("id").As("id"),
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

func (d *Database) AddUserUpdate(ctx context.Context, update *Update, opts ...QueryOption) (*Update, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	ds := db.Insert("updates").Rows(
		goqu.Record{
			"value_type":          update.ValueType,
			"value":               update.Value,
			"effective_date":      update.EffectiveDate,
			"update_operation_id": update.UpdateOperationID,
			"resource_type_id":    update.ResourceTypeID,
			"user_id":             update.UserID,
		},
	).
		Returning(goqu.C("id")).
		Executor()

	var id string

	if _, err = ds.ScanValContext(ctx, &id); err != nil {
		return nil, err
	}

	update.ID = id

	return update, nil
}

func (d *Database) GetCurrentUsage(ctx context.Context, resourceTypeID, userPlanID string, opts ...QueryOption) (float64, bool, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	usagesE := db.From("usages").
		Select(goqu.C("usage")).
		Where(goqu.And(
			goqu.I("resource_type_id").Eq(resourceTypeID),
			goqu.I("user_plan_id").Eq(userPlanID),
		)).
		Limit(1).
		Executor()

	var usageValue float64
	usageFound, err := usagesE.ScanValContext(ctx, &usageValue)
	if err != nil {
		return usageValue, false, err
	}

	return usageValue, usageFound, nil
}

func (d *Database) GetCurrentQuota(ctx context.Context, resourceTypeID, userPlanID string, opts ...QueryOption) (float64, bool, error) {
	var (
		err        error
		db         GoquDatabase
		quotaValue float64
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	quotasE := db.From("quotas").
		Select(goqu.C("quota")).
		Where(goqu.And(
			goqu.I("resource_type_id").Eq(resourceTypeID),
			goqu.I("user_plan_id").Eq(userPlanID),
		)).
		Limit(1).
		Executor()

	if _, err := quotasE.ScanValContext(ctx, &quotaValue); err != nil {
		return quotaValue, false, err
	}

	quotaFound, err := quotasE.ScanValContext(ctx, &quotaValue)
	if err != nil {
		return quotaValue, false, err
	}

	return quotaValue, quotaFound, nil
}

func (d *Database) UpsertUsage(ctx context.Context, update bool, value float64, resourceTypeID, userPlanID string, opts ...QueryOption) error {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	updateRecord := goqu.Record{
		"usage":            value,
		"resource_type_id": resourceTypeID,
		"user_plan_id":     userPlanID,
		"last_modified_by": "de",
		"created_by":       "de",
	}

	var upsertE exec.QueryExecutor
	if !update {
		upsertE = db.Insert("usages").Rows(updateRecord).Executor()
	} else {
		upsertE = db.Update("usages").Set(updateRecord).Where(
			goqu.And(
				goqu.I("resource_type_id").Eq(resourceTypeID),
				goqu.I("user_plan_id").Eq(userPlanID),
			),
		).Executor()
	}

	log.Info(upsertE.ToSQL())

	_, err = upsertE.ExecContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) UpsertQuota(ctx context.Context, update bool, value float64, resourceTypeID, userPlanID string, opts ...QueryOption) error {
	var (
		err error
		db  GoquDatabase
	)

	querySettings := &QuerySettings{}
	for _, opt := range opts {
		opt(querySettings)
	}

	if querySettings.tx != nil {
		db = querySettings.tx
	} else {
		db = d.goquDB
	}

	updateRecord := goqu.Record{
		"quota":            value,
		"resource_type_id": resourceTypeID,
		"user_plan_id":     userPlanID,
		"create_by":        "de",
		"last_modified_by": "de",
	}

	var upsertE exec.QueryExecutor
	if !update {
		upsertE = db.Insert("quotas").Rows(updateRecord).Executor()
	} else {
		upsertE = db.Update("quotas").Set(updateRecord).Where(
			goqu.And(
				goqu.I("resource_type_id").Eq(resourceTypeID),
				goqu.I("user_plan_id").Eq(userPlanID),
			),
		).Executor()
	}

	log.Info(upsertE.ToSQL())

	_, err = upsertE.ExecContext(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) ProcessUpdateForUsage(ctx context.Context, update *Update) error {
	log = log.WithFields(logrus.Fields{"context": "usage update", "user": update.Username})

	db := d.fullDB

	log.Debug("beginning transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	log.Debug("after beginning transaction")

	if err = tx.Wrap(func() error {
		log.Debug("before getting active user plan")
		userPlan, err := d.GetActiveUserPlan(ctx, update.Username, WithTX(tx))
		if err != nil {
			return err
		}
		log.Debugf("after getting active user plan %s", userPlan.ID)

		log.Debug("getting current usage")
		usageValue, usageFound, err := d.GetCurrentUsage(ctx, update.ResourceTypeID, userPlan.ID, WithTX(tx))
		if err != nil {
			return err
		}
		log.Debugf("done getting current usage of %f", usageValue)

		switch update.UpdateOperationName {
		case UpdateTypeSet:
			usageValue = update.Value
		case UpdateTypeAdd:
			usageValue = usageValue + update.Value
		default:
			return fmt.Errorf("invalid update type: %s", update.UpdateOperationName)
		}
		log.Debugf("new usage value is %f", usageValue)

		log.Debug("upserting new usage value")
		if err = d.UpsertUsage(ctx, usageFound, usageValue, update.ResourceTypeID, userPlan.ID, WithTX(tx)); err != nil {
			return err
		}
		log.Debug("done upserting new value")

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (d *Database) ProcessUpdateForQuota(ctx context.Context, update *Update, opts ...QueryOption) error {
	var err error

	db := d.fullDB

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err = tx.Wrap(func() error {
		userPlan, err := d.GetActiveUserPlan(ctx, update.Username, WithTX(tx))
		if err != nil {
			return err
		}

		quotaValue, quotaFound, err := d.GetCurrentQuota(ctx, update.ResourceTypeID, userPlan.ID, WithTX(tx))
		if err != nil {
			return err
		}

		switch update.UpdateOperationName {
		case UpdateTypeSet:
			quotaValue = update.Value
		case UpdateTypeAdd:
			quotaValue = quotaValue + update.Value
		default:
			return fmt.Errorf("invalid update type: %s", update.UpdateOperationName)
		}

		if err = d.UpsertQuota(
			ctx,
			quotaFound,
			quotaValue,
			update.ResourceTypeID,
			userPlan.ID,
			WithTX(tx),
		); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
