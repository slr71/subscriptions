package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exec"
)

// GetCurrentQuota returns the current quota value for a resource type and
// user plan. Also returns a boolean that is true when the actual quota value
// was found and returned and is false when the actual quota was not found and
// the default value was returned. Accepts a variable number of QuotaOptions,
// but only WithTX is currently supported.
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

// UpsertQuota inserts or updates a quota into the database for the given
// resource type and user plan. Accepts a variable number of QueryOptions,
// though only WithTX is currently supported.
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
		"created_by":       "de",
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
