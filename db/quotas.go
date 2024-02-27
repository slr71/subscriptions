package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

// GetCurrentQuota returns the current quota value for a resource type and
// user plan. Also returns a boolean that is true when the actual quota value
// was found and returned and is false when the actual quota was not found and
// the default value was returned. Accepts a variable number of QuotaOptions,
// but only WithTX is currently supported.
func (d *Database) GetCurrentQuota(ctx context.Context, resourceTypeID, subscriptionID string, opts ...QueryOption) (float64, bool, error) {
	var (
		err        error
		db         GoquDatabase
		quotaValue float64
	)

	_, db = d.querySettings(opts...)

	quotasE := db.From("quotas").
		Select(goqu.C("quota")).
		Where(goqu.And(
			goqu.I("resource_type_id").Eq(resourceTypeID),
			goqu.I("subscription_id").Eq(subscriptionID),
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
func (d *Database) UpsertQuota(ctx context.Context, value float64, resourceTypeID, subscriptionID string, opts ...QueryOption) error {
	var (
		err error
		db  GoquDatabase
	)

	_, db = d.querySettings(opts...)

	updateRecord := goqu.Record{
		"quota":            value,
		"resource_type_id": resourceTypeID,
		"subscription_id":  subscriptionID,
		"created_by":       "de",
		"last_modified_by": "de",
	}

	upsertE := db.Insert("quotas").
		Rows(updateRecord).
		OnConflict(
			goqu.DoUpdate(
				"resource_type_id, subscription_id",
				goqu.C("quota").Set(goqu.I("excluded.quota"))),
		).Executor()

	log.Info(upsertE.ToSQL())

	_, err = upsertE.ExecContext(ctx)
	if err != nil {
		return err
	}

	return nil
}
