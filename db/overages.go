// GetUserOverages returns a user's list of overages. Accepts a variable number
// of QueryOptions, though only WithTX is currently supported.
package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

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

			usersT.Col("id").As(goqu.C("users.id")),
			usersT.Col("username").As(goqu.C("users.username")),

			plansT.Col("id").As(goqu.C("plans.id")),
			plansT.Col("name").As(goqu.C("plans.name")),
			plansT.Col("description").As(goqu.C("plans.description")),

			rtT.Col("id").As(goqu.C("resource_types.id")),
			rtT.Col("name").As(goqu.C("resource_types.name")),
			rtT.Col("unit").As(goqu.C("resource_types.unit")),

			quotasT.Col("quota").As("quota_value"),
			usagesT.Col("usage").As("usage_value"),
		).
		Join(usersT, goqu.On(userPlansT.Col("user_id").Eq(usersT.Col("id")))).
		Join(plansT, goqu.On(userPlansT.Col("plan_id").Eq(plansT.Col("id")))).
		Join(quotasT, goqu.On(userPlansT.Col("id").Eq(quotasT.Col("user_plan_id")))).
		Join(usagesT, goqu.On(userPlansT.Col("id").Eq(usagesT.Col("user_plan_id")))).
		Join(rtT, goqu.On(usagesT.Col("resource_type_id").Eq(rtT.Col("id")))).
		Where(goqu.And(
			usersT.Col("username").Eq(username),
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
