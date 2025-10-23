// GetUserOverages returns a user's list of overages. Accepts a variable number
// of QueryOptions, though only WithTX is currently supported.
package db

import (
	"context"

	t "github.com/cyverse-de/subscriptions/db/tables"
	"github.com/doug-martin/goqu/v9"
)

func (d *Database) GetUserOverages(ctx context.Context, username string, opts ...QueryOption) ([]Overage, error) {
	var (
		err      error
		db       GoquDatabase
		overages []Overage
	)

	_, db = d.querySettings(opts...)

	query := db.From(t.Subscriptions).
		Select(
			t.Subscriptions.Col("id").As("subscription_id"),

			t.Users.Col("id").As(goqu.C("users.id")),
			t.Users.Col("username").As(goqu.C("users.username")),

			t.Plans.Col("id").As(goqu.C("plans.id")),
			t.Plans.Col("name").As(goqu.C("plans.name")),
			t.Plans.Col("description").As(goqu.C("plans.description")),

			t.ResourceTypes.Col("id").As(goqu.C("resource_types.id")),
			t.ResourceTypes.Col("name").As(goqu.C("resource_types.name")),
			t.ResourceTypes.Col("unit").As(goqu.C("resource_types.unit")),
			t.ResourceTypes.Col("consumable").As(goqu.C("resource_types.consumable")),

			t.Quotas.Col("quota").As("quota_value"),
			t.Usages.Col("usage").As("usage_value"),
		).
		Join(t.Users, goqu.On(t.Subscriptions.Col("user_id").Eq(t.Users.Col("id")))).
		Join(t.Plans, goqu.On(t.Subscriptions.Col("plan_id").Eq(t.Plans.Col("id")))).
		Join(t.Quotas, goqu.On(t.Subscriptions.Col("id").Eq(t.Quotas.Col("subscription_id")))).
		Join(t.Usages, goqu.On(t.Subscriptions.Col("id").Eq(t.Usages.Col("subscription_id")))).
		Join(t.ResourceTypes, goqu.On(t.Usages.Col("resource_type_id").Eq(t.ResourceTypes.Col("id")))).
		Where(goqu.And(
			t.Users.Col("username").Eq(username),
			goqu.Or(
				CurrentTimestamp.Between(goqu.Range(t.Subscriptions.Col("effective_start_date"), t.Subscriptions.Col("effective_end_date"))),
				goqu.And(
					CurrentTimestamp.Gt(t.Subscriptions.Col("effective_start_date")),
					t.Subscriptions.Col("effective_end_date").IsNull(),
				),
			),
			t.Usages.Col("resource_type_id").Eq(t.Quotas.Col("resource_type_id")),
			t.Usages.Col("usage").Gte(t.Quotas.Col("quota")),
		)).Executor()

	if err = query.ScanStructsContext(ctx, &overages); err != nil {
		return nil, err
	}

	return overages, nil
}
