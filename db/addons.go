package db

import (
	"context"

	t "github.com/cyverse-de/subscriptions/db/tables"
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
	if err := ds.ScanValsContext(ctx, &newAddonID); err != nil {
		return "", err
	}

	return newAddonID, nil
}

func (d *Database) ListAddons(ctx context.Context, opts ...QueryOption) ([]Addon, error) {
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
		return nil, errors.Wrap(err, "unable to list addons")
	}

	return addons, nil
}
