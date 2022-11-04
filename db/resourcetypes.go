package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

// GetResourceTypeID returns the UUID associated with the name and unit passed in.
// Accepts a variable number of QueryOptions, though only transactions are
// currently supported.
func (d *Database) GetResourceTypeID(ctx context.Context, name, unit string, opts ...QueryOption) (string, error) {
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

	rtT := goqu.T("resource_types")
	query := db.From(rtT).
		Select("id").
		Where(goqu.Ex{
			"name": name,
			"unit": unit,
		})
	qs, _, err := query.ToSQL()
	if err != nil {
		return "", err
	}
	var result string
	if _, err = db.ScanValContext(ctx, &result, qs); err != nil {
		return "", err
	}
	return result, nil
}

// GetResourceType returns a *ResourceType associated with the UUID passed in.
// Accepts a variable number of QueryOptions, though only transactions are
// currently supported.
func (d *Database) GetResourceType(ctx context.Context, id string, opts ...QueryOption) (*ResourceType, error) {
	var (
		err    error
		db     GoquDatabase
		result ResourceType
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

	rtT := goqu.T("resource_types")
	query := db.From(rtT).
		Select(
			rtT.Col("id"),
			rtT.Col("name"),
			rtT.Col("unit"),
		).
		Where(rtT.Col("id").Eq(id)).
		Executor()

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}

	return &result, err
}

// GetResourceTypeByName returns a *ResourceType associated with the name passed
// in. Accepts a variable number of QueryOptions, though only transactions are
// currently supported.
func (d *Database) GetResourceTypeByName(ctx context.Context, name string, opts ...QueryOption) (*ResourceType, error) {
	var (
		err          error
		db           GoquDatabase
		resourceType ResourceType
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

	rtT := goqu.T("resource_types")
	query := db.From(rtT).
		Select(
			rtT.Col("id"),
			rtT.Col("name"),
			rtT.Col("unit"),
		).
		Where(rtT.Col("name").Eq(name)).
		Executor()

	if _, err = query.ScanStructContext(ctx, &resourceType); err != nil {
		return nil, err
	}

	return &resourceType, nil
}
