package db

import (
	"context"
	"fmt"

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
	addonRateRows := make([]any, len(addon.AddonRates))
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
			t.ResourceTypes.Col("consumable").As(goqu.C("resource_types.consumable")),
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

	addonFound, err = addonInfo.ScanStructContext(ctx, addon)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get add-on info")
	} else if !addonFound {
		return nil, fmt.Errorf("addon ID %s not found", addonID)
	}

	addonRates, err := d.ListRatesForAddon(ctx, addonID, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get add-on info")
	}
	addon.AddonRates = addonRates

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
			t.ResourceTypes.Col("consumable").As(goqu.C("resource_types.consumable")),
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
		).
		Where(goqu.Ex{"addon_id": addonID}).
		Order(goqu.I("effective_date").Asc())
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
			t.ResourceTypes.Col("consumable").As(goqu.C("resource_types.consumable")),
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

func (d *Database) UpsertAddonRate(ctx context.Context, r AddonRate, opts ...QueryOption) error {
	_, db := d.querySettings(opts...)

	// Create the addon record.
	rec := r.ToRec()

	ds := db.Insert(t.AddonRates).
		Rows(rec).
		OnConflict(goqu.DoUpdate("id", rec)).
		Executor()
	if _, err := ds.ExecContext(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Database) UpdateAddonRates(ctx context.Context, addonUpdateRecord *UpdateAddon, opts ...QueryOption) error {
	_, db := d.querySettings(opts...)

	// Delete any existing addon rates that aren't mentioned in the incoming request.
	var addonRateIDs []string
	for _, r := range addonUpdateRecord.AddonRates {
		if r.ID != "" {
			addonRateIDs = append(addonRateIDs, r.ID)
		}
	}
	if len(addonRateIDs) != 0 {
		ds := db.From(t.AddonRates).
			Where(goqu.Ex{
				"addon_id": addonUpdateRecord.ID,
				"id":       goqu.Op{"notIn": addonRateIDs},
			}).
			Delete().
			Executor()
		if _, err := ds.ExecContext(ctx); err != nil {
			return err
		}
	}

	for _, r := range addonUpdateRecord.AddonRates {
		err := d.UpsertAddonRate(ctx, r, opts...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Database) UpdateAddon(ctx context.Context, addonUpdateRecord *UpdateAddon, opts ...QueryOption) error {
	_, db := d.querySettings(opts...)

	rec := goqu.Record{}

	updateAddon := false
	if addonUpdateRecord.UpdateName {
		rec["name"] = addonUpdateRecord.Name
		updateAddon = true
	}
	if addonUpdateRecord.UpdateDescription {
		rec["description"] = addonUpdateRecord.Description
		updateAddon = true
	}
	if addonUpdateRecord.UpdateResourceType {
		rec["resource_type_id"] = addonUpdateRecord.ResourceTypeID
		updateAddon = true
	}
	if addonUpdateRecord.UpdateDefaultAmount {
		rec["default_amount"] = addonUpdateRecord.DefaultAmount
		updateAddon = true
	}
	if addonUpdateRecord.UpdateDefaultPaid {
		rec["default_paid"] = addonUpdateRecord.DefaultPaid
		updateAddon = true
	}

	// Update the top-level addon record if requested.
	if updateAddon {
		ds := db.Update(t.Addons).
			Set(rec).
			Where(t.Addons.Col("id").Eq(addonUpdateRecord.ID)).
			Executor()

		r, err := ds.ExecContext(ctx)
		if err != nil {
			return errors.Wrap(err, "unable to execute the update")
		}
		rowsAffected, err := r.RowsAffected()
		if err != nil {
			return errors.Wrap(err, "unable to determine how many rows were affected")
		}
		if rowsAffected == 0 {
			return suberrors.ErrAddonNotFound
		}
	}

	// Update existing addon rates.
	if addonUpdateRecord.UpdateAddonRates {
		err := d.UpdateAddonRates(ctx, addonUpdateRecord, opts...)
		if err != nil {
			return errors.Wrap(err, "unable to update the addon rates for the addon")
		}
	}

	return nil
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
			t.ResourceTypes.Col("consumable").As(goqu.C("addons.resource_types.consumable")),

			t.SubscriptionAddons.Col("amount"),
			t.SubscriptionAddons.Col("paid"),
			t.SubscriptionAddons.Col("subscription_id"),

			t.AddonRates.Col("id").As(goqu.C("addon_rates.id")),
			t.AddonRates.Col("effective_date").As(goqu.C("addon_rates.effective_date")),
			t.AddonRates.Col("rate").As(goqu.C("addon_rates.rate")),
		).
		Join(t.Addons, goqu.On(t.Addons.Col("id").Eq(t.SubscriptionAddons.Col("addon_id")))).
		Join(t.ResourceTypes, goqu.On(t.Addons.Col("resource_type_id").Eq(t.ResourceTypes.Col("id")))).
		Join(t.AddonRates, goqu.On(t.SubscriptionAddons.Col("addon_rate_id").Eq(t.AddonRates.Col("id"))))
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

func (d *Database) ListSubscriptionAddons(
	ctx context.Context,
	subscriptionID string,
	opts ...QueryOption,
) ([]SubscriptionAddon, error) {
	_, db := d.querySettings(opts...)

	ds := subAddonDS(db).
		Where(t.SubscriptionAddons.Col("subscription_id").Eq(subscriptionID)).
		Executor()
	d.LogSQL(ds)

	var addons []SubscriptionAddon
	if err := ds.ScanStructsContext(ctx, &addons); err != nil {
		return nil, errors.Wrap(err, "unable to list addons")
	}

	return addons, nil
}

func (d *Database) AddSubscriptionAddon(
	ctx context.Context,
	subscriptionID, addonID string,
	opts ...QueryOption,
) (*SubscriptionAddon, error) {
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
	addonRate := addon.GetCurrentRate()
	if addonRate == nil {
		return nil, fmt.Errorf("no active rate found for addon %s", addon.ID)
	}

	ds := db.Insert(t.SubscriptionAddons).
		Rows(goqu.Record{
			"subscription_id": subscriptionID,
			"addon_id":        addonID,
			"amount":          addon.DefaultAmount,
			"paid":            addon.DefaultPaid,
			"addon_rate_id":   addonRate.ID,
		}).
		Returning(t.SubscriptionAddons.Col("id")).
		Executor()

	var newAddonID string
	if _, err = ds.ScanValContext(ctx, &newAddonID); err != nil {
		return nil, err
	}

	if qs.doCommit {
		if err = db.Commit(); err != nil {
			return nil, err
		}
	}

	retval := &SubscriptionAddon{
		ID:             newAddonID,
		Addon:          *addon,
		SubscriptionID: subscriptionID,
		Amount:         addon.DefaultAmount,
		Paid:           addon.DefaultPaid,
		Rate:           *addonRate,
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

	if qs.doCommit {
		if err = db.Commit(); err != nil {
			return nil, err
		}
	}

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
