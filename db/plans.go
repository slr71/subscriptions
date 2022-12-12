package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

func (d *Database) GetPlanByID(ctx context.Context, planID string, opts ...QueryOption) (*Plan, error) {
	var (
		err  error
		db   GoquDatabase
		plan Plan
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

	plans := goqu.T("plans")
	qd := goqu.T("plan_quota_defaults")
	rt := goqu.T("resource_types")

	query := db.From(plans).
		Select(
			plans.Col("id"),
			plans.Col("name"),
			plans.Col("description"),
			qd.Col("id").As(goqu.C("plan_quota_defaults.id")),
			qd.Col("plan_id").As(goqu.C("plan_quota_defaults.plan_id")),
			qd.Col("quota_value").As(goqu.C("plan_quota_defaults.quota_value")),
			qd.Col("resource_type_id").As(goqu.C("plan_quota_defaults.resource_type_id")),
			rt.Col("id").As(goqu.C("resource_types.id")),
			rt.Col("name").As(goqu.C("resource_types.name")),
			rt.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(qd, goqu.On(qd.Col("plan_id").Eq(plans.Col("id")))).
		Join(rt, goqu.On(qd.Col("resource_type_id").Eq(rt.Col("id")))).
		Where(
			plans.Col("id").Eq(planID),
		).
		Executor()

	if err = query.ScanStructsContext(ctx, &plan); err != nil {
		return nil, err
	}

	return &plan, err
}

func (d *Database) GetPlanByName(ctx context.Context, name string, opts ...QueryOption) (*Plan, error) {
	var (
		err  error
		db   GoquDatabase
		plan Plan
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

	plans := goqu.T("plans")
	qd := goqu.T("plan_quota_defaults")
	rt := goqu.T("resource_types")

	query := db.From(plans).
		Select(
			plans.Col("id"),
			plans.Col("name"),
			plans.Col("description"),
			qd.Col("id").As(goqu.C("plan_quota_defaults.id")),
			qd.Col("plan_id").As(goqu.C("plan_quota_defaults.plan_id")),
			qd.Col("quota_value").As(goqu.C("plan_quota_defaults.quota_value")),
			qd.Col("resource_type_id").As(goqu.C("plan_quota_defaults.resource_type_id")),
			rt.Col("id").As(goqu.C("resource_types.id")),
			rt.Col("name").As(goqu.C("resource_types.name")),
			rt.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(qd, goqu.On(qd.Col("plan_id").Eq(plans.Col("id")))).
		Join(rt, goqu.On(qd.Col("resource_type_id").Eq(rt.Col("id")))).
		Where(
			plans.Col("name").Eq(name),
		).
		Executor()

	if err = query.ScanStructsContext(ctx, &plan); err != nil {
		return nil, err
	}

	return &plan, err
}
