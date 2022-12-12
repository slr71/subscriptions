package db

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/sirupsen/logrus"
)

// UserUpdates returns a list of updates associated with a user.
// Accepts a variable number of QueryOptions, including WithTX, WithQueryLimit,
// and WithQueryOffset.
func (d *Database) UserUpdates(ctx context.Context, username string, opts ...QueryOption) ([]Update, error) {
	var (
		err error
		db  GoquDatabase
	)

	querySettings, db := d.querySettings(opts...)

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

// AddUserUpdate inserts the passed in update into the database. Returns the
// Update with the UUID filled in. Accepts a variable number of QueryOptions,
// though only WithTx is currently supported.
func (d *Database) AddUserUpdate(ctx context.Context, update *Update, opts ...QueryOption) (*Update, error) {
	var (
		err error
		db  GoquDatabase
	)

	_, db = d.querySettings(opts...)

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

// ProcessUpdateForUsage accepts a new *Update, inserts it into the database,
// then uses it to calculate new usage and upsert it into the database. Does not
// accept any QueryOptions since it sets up the transaction and other options
// itself.
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

// ProcessUpdateForQuota accepts a new *Update, inserts it into the database,
// then uses it to calculate a new usage value, which in turn is upserted into
// the database. Does not accept an QueryOptions since it sets up the
// transaction and other options itself.
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
