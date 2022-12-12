package db

import (
	"context"

	t "github.com/cyverse-de/subscriptions/db/tables"
	"github.com/doug-martin/goqu/v9"
)

func (d *Database) GetPlanByID(ctx context.Context, planID string, opts ...QueryOption) (*Plan, error) {
	var (
		err  error
		db   GoquDatabase
		plan Plan
	)

	_, db = d.querySettings(opts...)

	query := db.From(t.Plans).
		Select(
			t.Plans.Col("id"),
			t.Plans.Col("name"),
			t.Plans.Col("description"),

			t.PQD.Col("id").As(goqu.C("plan_quota_defaults.id")),
			t.PQD.Col("plan_id").As(goqu.C("plan_quota_defaults.plan_id")),
			t.PQD.Col("quota_value").As(goqu.C("plan_quota_defaults.quota_value")),
			t.PQD.Col("resource_type_id").As(goqu.C("plan_quota_defaults.resource_type_id")),

			t.RT.Col("id").As(goqu.C("resource_types.id")),
			t.RT.Col("name").As(goqu.C("resource_types.name")),
			t.RT.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(t.PQD, goqu.On(t.PQD.Col("plan_id").Eq(t.Plans.Col("id")))).
		Join(t.RT, goqu.On(t.PQD.Col("resource_type_id").Eq(t.RT.Col("id")))).
		Where(
			t.Plans.Col("id").Eq(planID),
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

	_, db = d.querySettings(opts...)

	query := db.From(t.Plans).
		Select(
			t.Plans.Col("id"),
			t.Plans.Col("name"),
			t.Plans.Col("description"),

			t.PQD.Col("id").As(goqu.C("plan_quota_defaults.id")),
			t.PQD.Col("plan_id").As(goqu.C("plan_quota_defaults.plan_id")),
			t.PQD.Col("quota_value").As(goqu.C("plan_quota_defaults.quota_value")),
			t.PQD.Col("resource_type_id").As(goqu.C("plan_quota_defaults.resource_type_id")),

			t.RT.Col("id").As(goqu.C("resource_types.id")),
			t.RT.Col("name").As(goqu.C("resource_types.name")),
			t.RT.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(t.PQD, goqu.On(t.PQD.Col("plan_id").Eq(t.Plans.Col("id")))).
		Join(t.RT, goqu.On(t.PQD.Col("resource_type_id").Eq(t.RT.Col("id")))).
		Where(
			t.Plans.Col("name").Eq(name),
		).
		Executor()

	if err = query.ScanStructsContext(ctx, &plan); err != nil {
		return nil, err
	}

	return &plan, err
}
