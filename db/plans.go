package db

import (
	"context"

	t "github.com/cyverse-de/subscriptions/db/tables"
	"github.com/doug-martin/goqu/v9"
)

func planDS(db GoquDatabase) *goqu.SelectDataset {
	return db.From(t.Plans).
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
		Join(t.RT, goqu.On(t.PQD.Col("resource_type_id").Eq(t.RT.Col("id"))))
}

func (d *Database) ListPlans(ctx context.Context, opts ...QueryOption) ([]Plan, error) {
	_, db := d.querySettings(opts...)

	query := planDS(db).Executor()

	var plans []Plan

	if err := query.ScanStructsContext(ctx, &plans); err != nil {
		return nil, err
	}

	return plans, nil
}

func (d *Database) GetPlanByID(ctx context.Context, planID string, opts ...QueryOption) (*Plan, error) {
	var (
		err  error
		db   GoquDatabase
		plan Plan
	)

	_, db = d.querySettings(opts...)

	query := planDS(db).
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

	query := planDS(db).
		Where(
			t.Plans.Col("name").Eq(name),
		).
		Executor()

	if err = query.ScanStructsContext(ctx, &plan); err != nil {
		return nil, err
	}

	return &plan, err
}

func (d *Database) AddPlan(ctx context.Context, plan *Plan, opts ...QueryOption) (string, error) {
	_, db := d.querySettings(opts...)

	ds := db.Insert(t.Plans).Rows(
		goqu.Record{
			"name":        plan.Name,
			"description": plan.Description,
		},
	).
		Returning(t.Plans.Col("id")).
		Executor()

	var newPlanID string
	if err := ds.ScanValsContext(ctx, newPlanID); err != nil {
		return "", err
	}

	for _, pqd := range plan.QuotaDefaults {
		newDS := db.Insert(t.PQD).
			Rows(
				goqu.Record{
					"plan_id":          newPlanID,
					"resource_type_id": pqd.ResourceType.ID,
					"quota_value":      pqd.QuotaValue,
				},
			).Executor()

		if _, err := newDS.ExecContext(ctx); err != nil {
			return "", err
		}
	}

	return newPlanID, nil
}
