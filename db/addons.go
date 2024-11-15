package db

import (
	"context"

	t "github.com/cyverse-de/subscriptions/db/tables"
	suberrors "github.com/cyverse-de/subscriptions/errors"
	"github.com/doug-martin/goqu/v9"
	"github.com/pkg/errors"
)

func (d *Database) AddAddon(ctx context.Context, addon *Addon, opts ...QueryOption) (string, error) {
	_, db := d.querySettings(opts...)

	ds := db.Insert(t.Addons).Rows(
		goqu.Record{
			"name":             addon.Name,
			"description":      addon.Description,
			"resource_type_id": addon.ResourceType.ID,
			"default_amount":   addon.DefaultAmount,
			"default_paid":     addon.DefaultPaid,
		},
	).
		Returning(t.Addons.Col("id")).
		Executor()

	var newAddonID string
	if _, err := ds.ScanValContext(ctx, &newAddonID); err != nil {
		return "", err
	}

	// Add the addon rates.
	addonRateRows := make([]interface{}, len(addon.AddonRates))
	for i, r := range addon.AddonRates {
		addonRateRows[i] = goqu.Record{
			"addon_id":       newAddonID,
			"effective_date": r.EffectiveDate,
			"rate":           r.Rate,
		}
	}
	addonRateDS := db.Insert(t.AddonRates).
		Rows(addonRateRows...).
		Executor()
	if _, err := addonRateDS.ExecContext(ctx); err != nil {
		return "", err
	}

	return newAddonID, nil
}

func addonDS(db GoquDatabase) *goqu.SelectDataset {
	return db.From(t.Addons).
		Select(
			t.Addons.Col("id"),
			t.Addons.Col("name"),
			t.Addons.Col("description"),
			t.Addons.Col("default_amount"),
			t.Addons.Col("default_paid"),

			t.ResourceTypes.Col("id").As(goqu.C("resource_types.id")),
			t.ResourceTypes.Col("name").As(goqu.C("resource_types.name")),
			t.ResourceTypes.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(t.ResourceTypes, goqu.On(t.Addons.Col("resource_type_id").Eq(t.ResourceTypes.Col("id"))))
}

func (d *Database) GetAddonByID(ctx context.Context, addonID string, opts ...QueryOption) (*Addon, error) {
	var err error
	var addonFound bool

	_, db := d.querySettings(opts...)

	addon := &Addon{}
	addonInfo := addonDS(db).
		Where(t.Addons.Col("id").Eq(addonID)).
		Executor()

	if addonFound, err = addonInfo.ScanStructContext(ctx, addon); err != nil || !addonFound {
		return nil, errors.Wrap(err, "unable to get add-on info")
	}

	return addon, nil
}

func (d *Database) ListAddons(ctx context.Context, opts ...QueryOption) ([]Addon, error) {
	wrapMsg := "unable to list addons"
	_, db := d.querySettings(opts...)

	ds := db.From(t.Addons).
		Select(
			t.Addons.Col("id"),
			t.Addons.Col("name"),
			t.Addons.Col("description"),
			t.Addons.Col("default_amount"),
			t.Addons.Col("default_paid"),

			t.ResourceTypes.Col("id").As(goqu.C("resource_types.id")),
			t.ResourceTypes.Col("name").As(goqu.C("resource_types.name")),
			t.ResourceTypes.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(t.ResourceTypes, goqu.On(t.Addons.Col("resource_type_id").Eq(t.ResourceTypes.Col("id"))))
	d.LogSQL(ds)

	var addons []Addon
	if err := ds.ScanStructsContext(ctx, &addons); err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	for i, addon := range addons {
		addonRates, err := d.ListRatesForAddon(ctx, addon.ID, opts...)
		if err != nil {
			return nil, errors.Wrap(err, wrapMsg)
		}
		addons[i].AddonRates = addonRates
	}

	return addons, nil
}

func (d *Database) ListRatesForAddon(ctx context.Context, addonID string, opts ...QueryOption) ([]AddonRate, error) {
	_, db := d.querySettings(opts...)

	ds := db.From(t.AddonRates).
		Select(
			t.AddonRates.Col("id"),
			t.AddonRates.Col("addon_id"),
			t.AddonRates.Col("effective_date"),
			t.AddonRates.Col("rate"),
		)
	d.LogSQL(ds)

	var addonRates []AddonRate
	if err := ds.ScanStructsContext(ctx, &addonRates); err != nil {
		return nil, errors.Wrapf(err, "unable to list rates for addon ID %s", addonID)
	}

	return addonRates, nil
}

func (d *Database) ToggleAddonPaid(ctx context.Context, addonID string, opts ...QueryOption) (*Addon, error) {
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	opts = append(opts, WithTX(tx))

	_, db := d.querySettings(opts...)

	ds1 := db.Update(t.Addons).
		Set(goqu.Record{"default_paid": goqu.L("NOT default_paid")}).
		Where(t.Addons.Col("id").Eq(addonID)).
		Executor()

	_, err = ds1.ExecContext(ctx)
	if err != nil {
		return nil, err
	}

	ds2 := db.From(t.Addons).
		Select(
			t.Addons.Col("id"),
			t.Addons.Col("name"),
			t.Addons.Col("description"),
			t.Addons.Col("default_amount"),
			t.Addons.Col("default_paid"),

			t.ResourceTypes.Col("id").As(goqu.C("resource_types.id")),
			t.ResourceTypes.Col("name").As(goqu.C("resource_types.name")),
			t.ResourceTypes.Col("unit").As(goqu.C("resource_types.unit")),
		).
		Join(t.ResourceTypes, goqu.On(t.Addons.Col("resource_type_id").Eq(t.ResourceTypes.Col("id")))).
		Where(t.Addons.Col("id").Eq(addonID)).
		Executor()

	d.LogSQL(ds2)

	retval := &Addon{}
	_, err = ds2.ScanStructContext(ctx, retval)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return retval, nil
}

func (d *Database) UpdateAddon(ctx context.Context, updatedAddon *UpdateAddon, opts ...QueryOption) (*Addon, error) {
	_, db := d.querySettings(opts...)

	rec := goqu.Record{}

	if updatedAddon.UpdateName {
		rec["name"] = updatedAddon.Name
	}
	if updatedAddon.UpdateDescription {
		rec["description"] = updatedAddon.Description
	}
	if updatedAddon.UpdateResourceType {
		rec["resource_type_id"] = updatedAddon.ResourceTypeID
	}
	if updatedAddon.UpdateDefaultAmount {
		rec["default_amount"] = updatedAddon.DefaultAmount
	}
	if updatedAddon.UpdateDefaultPaid {
		rec["default_paid"] = updatedAddon.DefaultPaid
	}

	ds := db.Update(t.Addons).
		Set(rec).
		Where(t.Addons.Col("id").Eq(updatedAddon.ID)).
		Returning(
			t.Addons.Col("id"),
			t.Addons.Col("name"),
			t.Addons.Col("description"),
			t.Addons.Col("default_amount"),
			t.Addons.Col("default_paid"),
			t.Addons.Col("resource_type_id").As(goqu.C("resource_types.id")),
		).
		Executor()

	retval := &Addon{}
	found, err := ds.ScanStructContext(ctx, retval)
	if err != nil {
		return nil, errors.Wrap(err, "unable to scan results of update")
	}

	if !found {
		return nil, suberrors.ErrAddonNotFound
	}

	return retval, nil
}

func (d *Database) DeleteAddon(ctx context.Context, addonID string, opts ...QueryOption) error {
	_, db := d.querySettings(opts...)

	ds := db.From(t.Addons).
		Delete().
		Where(t.Addons.Col("id").Eq(addonID)).
		Executor()

	_, err := ds.ExecContext(ctx)
	return err
}

func subAddonDS(db GoquDatabase) *goqu.SelectDataset {
	return db.From(t.SubscriptionAddons).
		Select(
			t.SubscriptionAddons.Col("id"),

			t.Addons.Col("id").As(goqu.C("addons.id")),
			t.Addons.Col("name").As(goqu.C("addons.name")),
			t.Addons.Col("description").As(goqu.C("addons.description")),
			t.Addons.Col("default_amount").As(goqu.C("addons.default_amount")),
			t.Addons.Col("default_paid").As(goqu.C("addons.default_paid")),
			t.ResourceTypes.Col("id").As(goqu.C("addons.resource_types.id")),
			t.ResourceTypes.Col("name").As(goqu.C("addons.resource_types.name")),
			t.ResourceTypes.Col("unit").As(goqu.C("addons.resource_types.unit")),

			t.Subscriptions.Col("id").As(goqu.C("subscriptions.id")),
			t.Subscriptions.Col("effective_start_date").As(goqu.C("subscriptions.effective_start_date")),
			t.Subscriptions.Col("effective_end_date").As(goqu.C("subscriptions.effective_end_date")),
			t.Subscriptions.Col("created_by").As(goqu.C("subscriptions.created_by")),
			t.Subscriptions.Col("created_at").As(goqu.C("subscriptions.created_at")),
			t.Subscriptions.Col("last_modified_by").As(goqu.C("subscriptions.last_modified_by")),
			t.Subscriptions.Col("last_modified_at").As(goqu.C("subscriptions.last_modified_at")),
			t.Subscriptions.Col("paid").As(goqu.C("subscriptions.paid")),
			t.Users.Col("id").As(goqu.C("subscriptions.users.id")),
			t.Users.Col("username").As(goqu.C("subscriptions.users.username")),
			t.Plans.Col("id").As(goqu.C("subscriptions.plans.id")),
			t.Plans.Col("name").As(goqu.C("subscriptions.plans.name")),
			t.Plans.Col("description").As(goqu.C("subscriptions.plans.description")),

			t.SubscriptionAddons.Col("amount"),
			t.SubscriptionAddons.Col("paid"),
		).
		Join(t.Subscriptions, goqu.On(t.SubscriptionAddons.Col("subscription_id").Eq(t.Subscriptions.Col("id")))).
		Join(t.Addons, goqu.On(t.Addons.Col("id").Eq(t.SubscriptionAddons.Col("addon_id")))).
		Join(t.ResourceTypes, goqu.On(t.Addons.Col("resource_type_id").Eq(t.ResourceTypes.Col("id")))).
		Join(t.Users, goqu.On(t.Subscriptions.Col("user_id").Eq(t.Users.Col("id")))).
		Join(t.Plans, goqu.On(t.Subscriptions.Col("plan_id").Eq(t.Plans.Col("id"))))
}

func (d *Database) GetSubscriptionAddonByID(ctx context.Context, subAddonID string, opts ...QueryOption) (*SubscriptionAddon, error) {
	_, db := d.querySettings(opts...)

	ds := subAddonDS(db).
		Where(t.SubscriptionAddons.Col("id").Eq(subAddonID)).
		Executor()
	d.LogSQL(ds)

	subAddon := &SubscriptionAddon{}
	found, err := ds.ScanStructContext(ctx, subAddon)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, suberrors.ErrSubAddonNotFound
	}

	return subAddon, nil
}

func (d *Database) ListSubscriptionAddons(ctx context.Context, subscriptionID string, opts ...QueryOption) ([]SubscriptionAddon, error) {
	_, db := d.querySettings(opts...)

	ds := subAddonDS(db).
		Where(t.Subscriptions.Col("id").Eq(subscriptionID)).
		Executor()
	d.LogSQL(ds)

	var addons []SubscriptionAddon
	if err := ds.ScanStructsContext(ctx, &addons); err != nil {
		return nil, errors.Wrap(err, "unable to list addons")
	}

	return addons, nil
}

func (d *Database) AddSubscriptionAddon(ctx context.Context, subscriptionID, addonID string, opts ...QueryOption) (*SubscriptionAddon, error) {
	qs, db, err := d.querySettingsWithTX(opts...)
	if err != nil {
		return nil, err
	}

	if qs.doRollback {
		defer func() {
			if err := db.Rollback(); err != nil {
				log.Errorf("unable to roll back the transaction: %s", err)
			}
		}()
	}

	addon, err := d.GetAddonByID(ctx, addonID, WithTXRollbackCommit(db, false, false))
	if err != nil {
		return nil, err
	}

	ds := db.Insert(t.SubscriptionAddons).
		Rows(goqu.Record{
			"subscription_id": subscriptionID,
			"addon_id":        addonID,
			"amount":          addon.DefaultAmount,
			"paid":            addon.DefaultPaid,
		}).
		Returning(t.SubscriptionAddons.Col("id")).
		Executor()

	var newAddonID string
	if _, err = ds.ScanValContext(ctx, &newAddonID); err != nil {
		return nil, err
	}

	subscription, err := d.GetSubscriptionByID(ctx, subscriptionID, WithTXRollbackCommit(db, false, false))
	if err != nil {
		return nil, err
	}

	if qs.doCommit {
		if err = db.Commit(); err != nil {
			return nil, err
		}
	}

	retval := &SubscriptionAddon{
		ID:           newAddonID,
		Addon:        *addon,
		Subscription: *subscription,
		Amount:       addon.DefaultAmount,
		Paid:         addon.DefaultPaid,
	}

	return retval, nil
}

func (d *Database) DeleteSubscriptionAddon(ctx context.Context, subAddonID string, opts ...QueryOption) error {
	_, db := d.querySettings(opts...)

	ds := db.From(t.SubscriptionAddons).
		Delete().
		Where(t.SubscriptionAddons.Col("id").Eq(subAddonID)).
		Executor()

	_, err := ds.ExecContext(ctx)
	return err
}

func (d *Database) UpdateSubscriptionAddon(ctx context.Context, updated *UpdateSubscriptionAddon, opts ...QueryOption) (*SubscriptionAddon, error) {
	qs, db, err := d.querySettingsWithTX(opts...)
	if err != nil {
		return nil, err
	}

	if qs.doRollback {
		defer func() {
			if err := db.Rollback(); err != nil {
				log.Errorf("unable to roll back the transaction: %s", err)
			}
		}()
	}

	rec := goqu.Record{}
	if updated.UpdateAmount {
		rec["amount"] = updated.Amount
	}
	if updated.UpdatePaid {
		rec["paid"] = updated.Paid
	}

	ds := db.Update(t.SubscriptionAddons).
		Set(rec).
		Where(t.SubscriptionAddons.Col("id").Eq(updated.ID)).
		Returning(t.SubscriptionAddons.Col("id")).
		Executor()

	var id string
	found, err := ds.ScanValContext(ctx, &id)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, suberrors.ErrSubAddonNotFound
	}

	retval, err := d.GetSubscriptionAddonByID(ctx, updated.ID, WithTXRollbackCommit(db, false, false))
	if err != nil {
		return nil, err
	}

	usages, err := d.SubscriptionUsages(ctx, retval.Subscription.ID, WithTXRollbackCommit(db, false, false))
	if err != nil {
		return nil, err
	}

	quotas, err := d.SubscriptionQuotas(ctx, retval.Subscription.ID, WithTXRollbackCommit(db, false, false))
	if err != nil {
		return nil, err
	}

	pqd, err := d.SubscriptionQuotaDefaults(ctx, retval.Subscription.Plan.ID, WithTXRollbackCommit(db, false, false))
	if err != nil {
		return nil, err
	}

	if qs.doCommit {
		if err = db.Commit(); err != nil {
			return nil, err
		}
	}

	retval.Subscription.Usages = usages
	retval.Subscription.Quotas = quotas
	retval.Subscription.Plan.QuotaDefaults = pqd

	return retval, nil
}

func (d *Database) ListSubscriptionAddonsByAddonID(ctx context.Context, addonID string, opts ...QueryOption) ([]SubscriptionAddon, error) {
	_, db := d.querySettings(opts...)

	ds := subAddonDS(db).
		Where(t.Addons.Col("id").Eq(addonID)).
		Executor()
	d.LogSQL(ds)

	var addons []SubscriptionAddon
	if err := ds.ScanStructsContext(ctx, &addons); err != nil {
		return nil, errors.Wrap(err, "unable to list addons")
	}

	return addons, nil
}
