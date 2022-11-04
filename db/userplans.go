package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

// GetActiveUserPlan returns the active user plan for the username passed in.
// Accepts a variable number of QueryOptions, but only WithTX is currently
// supported.
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

// UserPlanUsages returns a list of Usages associated with a user plan specified
// by the passed in UUID. Accepts a variable number of QueryOptions, though only
// WithTX is currently supported.
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
			usagesT.Col("user_plan_id").As("user_plan_id"),
			usagesT.Col("created_by").As("created_by"),
			usagesT.Col("created_at").As("created_at"),
			usagesT.Col("last_modified_by").As("last_modified_by"),
			usagesT.Col("last_modified_at").As("last_modified_at"),
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

// UserPlanQuotas returns a list of Quotas associated with the user plan specified
// by the UUID passed in. Accepts a variable number of QueryOptions, though only
// WithTX is currently supported.
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

// UserPlanQuotaDefaults returns a list of PlanQuotaDefaults associated with the
// plan (not user plan, just plan) specified by the UUID passed in. Accepts a
// variable number of QueryOptions, though only WithTX is currently supported.
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

// UserPlanDetails returns lists of PlanQuotaDefaults, Quotas, and Usages
// Associated with the *UserPlan passed in. Accepts a variable number of
// QuotaOptions, though only WithTX is currently supported.
func (d *Database) UserPlanDetails(ctx context.Context, userPlan *UserPlan, opts ...QueryOption) ([]PlanQuotaDefault, []Quota, []Usage, error) {
	var (
		err      error
		defaults []PlanQuotaDefault
		usages   []Usage
		quotas   []Quota
	)

	defaults, err = d.UserPlanQuotaDefaults(ctx, userPlan.Plan.ID, opts...)
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
