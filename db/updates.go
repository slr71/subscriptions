package db

import (
	"context"
	"fmt"

	t "github.com/cyverse-de/subscriptions/db/tables"
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

	query := db.From(t.Updates).
		Select(
			t.Updates.Col("id"),
			t.Updates.Col("value_type"),
			t.Updates.Col("value"),
			t.Updates.Col("effective_date"),
			t.Updates.Col("created_by"),
			t.Updates.Col("created_at"),
			t.Updates.Col("last_modified_by"),
			t.Updates.Col("last_modified_at"),

			t.Users.Col("id").As(goqu.C("users.id")),
			t.Users.Col("username").As(goqu.C("users.username")),

			t.RT.Col("id").As(goqu.C("resource_types.id")),
			t.RT.Col("name").As(goqu.C("resource_types.name")),
			t.RT.Col("unit").As(goqu.C("resource_types.unit")),

			t.UOps.Col("id").As(goqu.C("update_operations.id")),
			t.UOps.Col("name").As(goqu.C("update_operations.name")),
		).
		Join(t.Users, goqu.On(goqu.I("updates.user_id").Eq(goqu.I("users.id")))).
		Join(t.UOps, goqu.On(goqu.I("updates.update_operation_id").Eq(goqu.I("update_operations.id")))).
		Join(t.RT, goqu.On(goqu.I("updates.resource_type_id").Eq(goqu.I("resource_types.id")))).
		Where(t.Users.Col("username").Eq(username))

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
		subscription, err := d.GetActiveSubscription(ctx, update.User.Username, WithTX(tx))
		if err != nil {
			return err
		}
		log.Debugf("after getting active user plan %s", subscription.ID)

		// create a subscription if there isn't one
		if subscription.ID == "" {
			user, err := d.EnsureUser(ctx, update.User.Username, WithTX(tx))
			if err != nil {
				log.Errorf("unable to ensure that the user exists in the database: %s", err)
				return err
			}

			plan, err := d.GetPlanByName(ctx, DefaultPlanName, WithTX(tx))
			if err != nil {
				log.Errorf("unable to look up the default plan: %s", err)
				return err
			}

			opts := DefaultSubscriptionOptions()
			subscriptionID, err := d.SetActiveSubscription(ctx, user.ID, plan, opts, WithTX(tx))
			if err != nil {
				log.Errorf("unable to subscribe the user to the default plan: %s", err)
				return err
			}

			subscription, err = d.GetSubscriptionByID(ctx, subscriptionID, WithTX(tx))
			if err != nil {
				log.Errorf("unable to look up the new user plan: %s", err)
				return err
			}
			if subscription == nil {
				err = fmt.Errorf("the newly inserted user plan could not be found")
				log.Error(err)
				return err
			}
		}

		log.Debug("getting current usage")
		usageValue, usageFound, err := d.GetCurrentUsage(ctx, update.ResourceType.ID, subscription.ID, WithTX(tx))
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
		if err = d.UpsertUsage(ctx, usageFound, usageValue, update.ResourceType.ID, subscription.ID, WithTX(tx)); err != nil {
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
		subscription, err := d.GetActiveSubscription(ctx, update.User.Username, WithTX(tx))
		if err != nil {
			return err
		}

		quotaValue, _, err := d.GetCurrentQuota(ctx, update.ResourceType.ID, subscription.ID, WithTX(tx))
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
			quotaValue,
			update.ResourceType.ID,
			subscription.ID,
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
