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
			updatesT.Col("value_type"),
			updatesT.Col("value"),
			updatesT.Col("effective_date"),
			updatesT.Col("created_by"),
			updatesT.Col("created_at"),
			updatesT.Col("last_modified_by"),
			updatesT.Col("last_modified_at"),

			usersT.Col("id").As(goqu.C("users.id")),
			usersT.Col("username").As(goqu.C("users.username")),

			rtT.Col("id").As(goqu.C("resource_types.id")),
			rtT.Col("name").As(goqu.C("resource_types.name")),
			rtT.Col("unit").As(goqu.C("resource_types.unit")),

			opsT.Col("id").As(goqu.C("update_operations.id")),
			opsT.Col("name").As(goqu.C("update_operations.name")),
		).
		Join(usersT, goqu.On(goqu.I("updates.user_id").Eq(goqu.I("users.id")))).
		Join(opsT, goqu.On(goqu.I("updates.update_operation_id").Eq(goqu.I("update_operations.id")))).
		Join(rtT, goqu.On(goqu.I("updates.resource_type_id").Eq(goqu.I("resource_types.id")))).
		Where(usersT.Col("username").Eq(username))

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

func (d *Database) GetResourceTypeByName(ctx context.Context, name string, opts ...QueryOption) (*ResourceType, error) {
	var (
		err          error
		db           GoquDatabase
		resourceType ResourceType
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
		Where(rtT.Col("name").Eq(name)).
		Executor()

	if _, err = query.ScanStructContext(ctx, &resourceType); err != nil {
		return nil, err
	}

	return &resourceType, nil
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
	plansT := goqu.T("plans")

	effStartDate := goqu.I("user_plans.effective_start_date")
	effEndDate := goqu.I("user_plans.effective_end_date")
	currTS := goqu.L("CURRENT_TIMESTAMP")

	query := db.From(userPlansT).
		Select(
			userPlansT.Col("id").As("id"),
			userPlansT.Col("effective_start_date").As("effective_start_date"),
			userPlansT.Col("effective_end_date").As("effective_end_date"),
			userPlansT.Col("created_by").As("created_by"),
			userPlansT.Col("created_at").As("created_at"),
			userPlansT.Col("last_modified_by").As("last_modified_by"),
			userPlansT.Col("last_modified_at").As("last_modified_at"),

			usersT.Col("id").As(goqu.C("users.id")),
			usersT.Col("username").As(goqu.C("users.username")),

			plansT.Col("id").As(goqu.C("plans.id")),
			plansT.Col("name").As(goqu.C("plans.name")),
			plansT.Col("description").As(goqu.C("plans.description")),
		).
		Join(usersT, goqu.On(goqu.I("user_plans.user_id").Eq(goqu.I("users.id")))).
		Join(plansT, goqu.On(goqu.I("user_plans.plan_id").Eq(goqu.I("plans.id")))).
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

func (d *Database) UserPlanUsages(ctx context.Context, userPlanID string, opts ...QueryOption) ([]Usage, error) {
	var (
		err    error
		db     GoquDatabase
		usages []Usage
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

	usagesT := goqu.T("usages")
	rtT := goqu.T("resource_types")

	usagesQuery := db.From(usagesT).
		Select(
			usagesT.Col("id").As("id"),
			usagesT.Col("quota").As("quota"),
			usagesT.Col("created_by").As("created_by"),
			usagesT.Col("created_at").As("created_at"),
			rtT.Col("id").As("resource_types.id"),
			rtT.Col("name").As("resource_types.name"),
			rtT.Col("unit").As("resource_types.unit"),
		).
		Join(rtT, goqu.On(goqu.I("usages.resource_type_id").Eq(goqu.I("resource_types.id")))).
		Where(usagesT.Col("user_plan_id").Eq(userPlanID)).
		Executor()

	if err = usagesQuery.ScanStructsContext(ctx, &usages); err != nil {
		return nil, err
	}

	return usages, nil
}

func (d *Database) UserPlanQuotas(ctx context.Context, userPlanID string, opts ...QueryOption) ([]Quota, error) {
	var (
		err    error
		db     GoquDatabase
		quotas []Quota
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
	quotasT := goqu.T("quotas")

	quotasQuery := db.From(quotasT).
		Select(
			quotasT.Col("id").As("id"),
			quotasT.Col("quota").As("quota"),
			quotasT.Col("created_by").As("created_by"),
			quotasT.Col("created_at").As("created_at"),
			quotasT.Col("last_modified_by").As("last_modified_by"),
			quotasT.Col("last_modified_at").As("last_modified_at"),
			rtT.Col("id").As("resource_types.id"),
			rtT.Col("name").As("resource_types.name"),
			rtT.Col("unit").As("resource_types.unit"),
		).
		Join(rtT, goqu.On(goqu.I("quotas.resource_type_id").Eq(goqu.I("resource_types.id")))).
		Where(quotasT.Col("user_plan_id").Eq(userPlanID)).
		Executor()

	if err = quotasQuery.ScanStructsContext(ctx, &quotas); err != nil {
		return nil, err
	}

	return quotas, nil
}

func (d *Database) UserPlanQuotaDefaults(ctx context.Context, planID string, opts ...QueryOption) ([]PlanQuotaDefault, error) {
	var (
		err      error
		db       GoquDatabase
		defaults []PlanQuotaDefault
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

	pqdT := goqu.T("plan_quota_defaults")
	rtT := goqu.T("resource_types")

	pqdQuery := db.From(pqdT).
		Select(
			pqdT.Col("id").As("id"),
			pqdT.Col("quota_value").As("quota_value"),
			pqdT.Col("plan_id").As("plan_id"),
			rtT.Col("id").As("resource_types.id"),
			rtT.Col("name").As("resource_types.name"),
			rtT.Col("unit").As("resource_types.unit"),
		).
		Join(rtT, goqu.On(goqu.I("plan_quota_defaults.resource_type_id").Eq(goqu.I("resource_types.id")))).
		Where(pqdT.Col("plan_id").Eq(planID)).
		Executor()

	if err = pqdQuery.ScanStructsContext(ctx, &defaults); err != nil {
		return nil, err
	}

	return defaults, nil
}

func (d *Database) UserPlanDetails(ctx context.Context, userPlan *UserPlan, opts ...QueryOption) ([]PlanQuotaDefault, []Quota, []Usage, error) {
	var (
		err      error
		defaults []PlanQuotaDefault
		usages   []Usage
		quotas   []Quota
	)

	defaults, err = d.UserPlanQuotaDefaults(ctx, *userPlan.Plan.ID, opts...)
	if err != nil {
		return nil, nil, nil, err
	}

	quotas, err = d.UserPlanQuotas(ctx, userPlan.ID, opts...)
	if err != nil {
		return nil, nil, nil, err
	}

	usages, err = d.UserPlanUsages(ctx, userPlan.ID, opts...)
	if err != nil {
		return nil, nil, nil, err
	}

	return defaults, quotas, usages, nil
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
			"update_operation_id": update.UpdateOperation.ID,
			"resource_type_id":    update.ResourceType.ID,
			"user_id":             update.User.ID,
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
	log = log.WithFields(logrus.Fields{"context": "usage update", "user": update.User.Username})

	db := d.fullDB

	log.Debug("beginning transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	log.Debug("after beginning transaction")

	if err = tx.Wrap(func() error {
		log.Debug("before getting active user plan")
		userPlan, err := d.GetActiveUserPlan(ctx, update.User.Username, WithTX(tx))
		if err != nil {
			return err
		}
		log.Debugf("after getting active user plan %s", userPlan.ID)

		log.Debug("getting current usage")
		usageValue, usageFound, err := d.GetCurrentUsage(ctx, update.ResourceType.ID, userPlan.ID, WithTX(tx))
		if err != nil {
			return err
		}
		log.Debugf("done getting current usage of %f", usageValue)

		log.Debugf("update operation name is %s", update.UpdateOperation.Name)
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
		if err = d.UpsertUsage(ctx, usageFound, usageValue, update.ResourceType.ID, userPlan.ID, WithTX(tx)); err != nil {
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
		userPlan, err := d.GetActiveUserPlan(ctx, update.User.Username, WithTX(tx))
		if err != nil {
			return err
		}

		quotaValue, quotaFound, err := d.GetCurrentQuota(ctx, update.ResourceType.ID, userPlan.ID, WithTX(tx))
		if err != nil {
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

		if err = d.UpsertQuota(
			ctx,
			quotaFound,
			quotaValue,
			update.ResourceType.ID,
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

func (d *Database) AddUsage(ctx context.Context, updateType string, usage *Usage, opts ...QueryOption) error {
	var (
		err           error
		newUsageValue float64
	)

	currentUsageValue, doUpdate, err := d.GetCurrentUsage(ctx, usage.ResourceType.ID, usage.UserPlanID, opts...)
	if err != nil {
		return err
	}
	log.Debugf("the current usage value is %f", currentUsageValue)

	switch updateType {
	case UpdateTypeSet:
		newUsageValue = usage.Usage
	case UpdateTypeAdd:
		newUsageValue = currentUsageValue + usage.Usage
	default:
		return fmt.Errorf("invalid update type: %s", updateType)
	}

	usage.Usage = newUsageValue

	if err = d.UpsertUsage(ctx, doUpdate, newUsageValue, usage.ResourceType.ID, usage.UserPlanID, opts...); err != nil {
		return err
	}

	return nil
}

func (d *Database) GetUserOverages(ctx context.Context, username string, opts ...QueryOption) ([]Overage, error) {
	var (
		err      error
		db       GoquDatabase
		overages []Overage
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
	userPlansT := goqu.T("user_plans")
	plansT := goqu.T("plans")
	rtT := goqu.T("resource_types")
	quotasT := goqu.T("quotas")
	usagesT := goqu.T("usages")
	ct := goqu.L("CURRENT_TIMESTAMP")

	query := db.From(userPlansT).
		Select(
			userPlansT.Col("id").As("user_plan_id"),

			usersT.Col("id").As("users.id"),
			usersT.Col("username").As("users.username"),

			plansT.Col("id").As("plans.id"),
			plansT.Col("name").As("plans.name"),
			plansT.Col("description").As("plans.description"),

			rtT.Col("id").As("resource_types.id"),
			rtT.Col("name").As("resource_types.name"),
			rtT.Col("unit").As("resource_types.unit"),

			quotasT.Col("quota").As("quota_value"),
			usagesT.Col("usage").As("usage_value"),
		).
		Join(usersT, goqu.On(userPlansT.Col("user_id").Eq(usersT.Col("id")))).
		Join(plansT, goqu.On(userPlansT.Col("plan_id").Eq(plansT.Col("id")))).
		Join(quotasT, goqu.On(userPlansT.Col("id").Eq(quotasT.Col("user_plan_id")))).
		Join(usagesT, goqu.On(userPlansT.Col("id").Eq(usagesT.Col("user_plan_id")))).
		Join(rtT, goqu.On(usagesT.Col("resource_type_id").Eq(rtT.Col("id")))).
		Where(goqu.And(
			usagesT.Col("username").Eq(username),
			goqu.Or(
				ct.Between(goqu.Range(userPlansT.Col("effective_start_date"), userPlansT.Col("effective_end_date"))),
				goqu.And(
					ct.Gt(userPlansT.Col("effective_start_date")),
					userPlansT.Col("effective_end_date").IsNull(),
				),
			),
		)).Executor()

	if err = query.ScanStructsContext(ctx, &overages); err != nil {
		return nil, err
	}

	return overages, nil
}
