package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

// GetOperationID returns the UUID associated with the operation name passed in.
// Accepts a variable number of QueryOptions, though only transactions are
// currently supported.
func (d *Database) GetOperationID(ctx context.Context, name string, opts ...QueryOption) (string, error) {
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

	opT := goqu.T("update_operations")
	query := db.From(opT).
		Select("id").
		Where(goqu.Ex{
			"name": name,
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

// GetOperation returns a *UpdateOperation associated with the UUID passed in.
// Accepts a variable number of QueryOptions, though only transactions are
// currently supported.
func (d *Database) GetOperation(ctx context.Context, id string, opts ...QueryOption) (*UpdateOperation, error) {
	var (
		err    error
		db     GoquDatabase
		result UpdateOperation
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

	opT := goqu.T("update_operations")
	query := db.From(opT).
		Select("id", "name").
		Where(goqu.Ex{
			"id": id,
		}).
		Executor()

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}

	return &result, err
}
