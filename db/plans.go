package db

import (
	"context"
	"fmt"

	t "github.com/cyverse-de/subscriptions/db/tables"
	"github.com/doug-martin/goqu/v9"
	"github.com/pkg/errors"
)

func planQuotaDefaultsDS(db GoquDatabase, planID string) *goqu.SelectDataset {
	return db.From(t.PQD).
		Select(
			t.PQD.Col("id"),
			t.PQD.Col("plan_id"),
			t.PQD.Col("quota_value"),
			t.PQD.Col("effective_date"),

			t.RT.Col("id").As(goqu.C("resource_types.id")),
			t.RT.Col("name").As(goqu.C("resource_types.name")),
			t.RT.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(t.RT, goqu.On(t.PQD.Col("resource_type_id").Eq(t.RT.Col("id")))).
		Where(t.PQD.Col("plan_id").Eq(planID)).
		Order(t.PQD.Col("effective_date").Asc(), t.RT.Col("name").Asc())
}

func planRatesDS(db GoquDatabase, planID string) *goqu.SelectDataset {
	return db.From(t.PlanRates).
		Select(
			t.PlanRates.Col("id"),
			t.PlanRates.Col("effective_date"),
			t.PlanRates.Col("rate"),
		).
		Where(t.PlanRates.Col("plan_id").Eq(planID)).
		Order(t.PlanRates.Col("effective_date").Asc())
}

func (d *Database) getPlanList(ctx context.Context, opts ...QueryOption) ([]Plan, error) {
	wrapMsg := "unable to list the plans"
	_, db := d.querySettings(opts...)

	// Build the query.
	query := db.From(t.Plans)
	d.LogSQL(query)

	// Execute the query and scan the results.
	var plans []Plan
	if err := query.ScanStructsContext(ctx, &plans); err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	return plans, nil
}

func (d *Database) loadPlanQuotaDefaults(ctx context.Context, plan *Plan, opts ...QueryOption) error {
	wrapMsg := fmt.Sprintf("unable to load the plan quota defaults for plan ID %s", plan.ID)
	_, db := d.querySettings(opts...)

	// Build the query.
	query := planQuotaDefaultsDS(db, plan.ID)
	d.LogSQL(query)

	// Execute the query and scan the results.
	err := query.ScanStructsContext(ctx, &plan.QuotaDefaults)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	return nil
}

func (d *Database) loadPlanRates(ctx context.Context, plan *Plan, opts ...QueryOption) error {
	wrapMsg := fmt.Sprintf("unable to load the plan rates for plan ID %s", plan.ID)
	_, db := d.querySettings(opts...)

	// Build the query.
	query := planRatesDS(db, plan.ID)
	d.LogSQL(query)

	// Execute the query and scan the results.
	err := query.ScanStructsContext(ctx, &plan.Rates)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	return nil
}

func (d *Database) loadPlanDetails(ctx context.Context, plan *Plan, opts ...QueryOption) error {
	err := d.loadPlanQuotaDefaults(ctx, plan, opts...)
	if err != nil {
		return err
	}

	err = d.loadPlanRates(ctx, plan, opts...)
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) ListPlans(ctx context.Context, opts ...QueryOption) ([]Plan, error) {
	// Get the list of plans.
	plans, err := d.getPlanList(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Load the details for each plan in the list.
	for i := range plans {
		err = d.loadPlanDetails(ctx, &plans[i], opts...)
		if err != nil {
			return nil, err
		}
	}

	return plans, nil
}

func (d *Database) GetPlanByID(ctx context.Context, planID string, opts ...QueryOption) (*Plan, error) {
	wrapMsg := fmt.Sprintf("unable to look up plan %s", planID)
	_, db := d.querySettings(opts...)

	// Build the query.
	query := db.From(t.Plans).Where(t.Plans.Col("id").Eq(planID))
	d.LogSQL(query)

	// Execute the query and scan the results.
	var plan Plan
	found, err := query.Executor().ScanStructContext(ctx, &plan)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}
	if !found {
		return nil, nil
	}

	// Load the plan details.
	err = d.loadPlanDetails(ctx, &plan, opts...)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	return &plan, nil
}

func (d *Database) GetPlanByName(ctx context.Context, name string, opts ...QueryOption) (*Plan, error) {
	wrapMsg := fmt.Sprintf("unable to look up plan %s", name)
	_, db := d.querySettings(opts...)

	// Build the query.
	query := db.From(t.Plans).Where(t.Plans.Col("name").Eq(name))
	d.LogSQL(query)

	// Execute the query and scan the results.
	var plan Plan
	found, err := query.Executor().ScanStructContext(ctx, &plan)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}
	if !found {
		return nil, nil
	}

	// Load the plan details.
	err = d.loadPlanDetails(ctx, &plan, opts...)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	return &plan, nil
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
	if _, err := ds.ScanValContext(ctx, &newPlanID); err != nil {
		return "", errors.Wrap(err, "unable to add the new plan")
	}

	for _, pqd := range plan.QuotaDefaults {
		pqdDS := db.Insert(t.PQD).
			Rows(
				goqu.Record{
					"plan_id":          newPlanID,
					"resource_type_id": pqd.ResourceType.ID,
					"quota_value":      pqd.QuotaValue,
					"effective_date":   pqd.EffectiveDate,
				},
			).Executor()

		if _, err := pqdDS.ExecContext(ctx); err != nil {
			return "", errors.Wrap(err, "unable to add plan quota defaults")
		}
	}

	for _, pr := range plan.Rates {
		prDS := db.Insert(t.PlanRates).
			Rows(
				goqu.Record{
					"plan_id":        newPlanID,
					"effective_date": pr.EffectiveDate,
					"rate":           pr.Rate,
				},
			).Executor()

		if _, err := prDS.ExecContext(ctx); err != nil {
			return "", errors.Wrap(err, "unable to add plan rates")
		}
	}

	return newPlanID, nil
}
