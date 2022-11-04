package db

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

// GetUserID returns a user's UUID associated with their username.
// Accepts a variable number of QueryOptions, though only WithTX is currently
// supported.
func (d *Database) GetUserID(ctx context.Context, username string, opts ...QueryOption) (string, error) {
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

	usersT := goqu.T("users")
	query := db.From(usersT).
		Select("id").
		Where(goqu.Ex{
			"username": username,
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

// GetUser returns a *User assocated with the UUID passed in.
// Accepts a variable number of QueryOptions, though online WithTX is currently
// supported.
func (d *Database) GetUser(ctx context.Context, id string, opts ...QueryOption) (*User, error) {
	var (
		err    error
		db     GoquDatabase
		result User
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
	query := db.From(usersT).
		Select("id", "username").
		Where(goqu.Ex{
			"id": id,
		}).
		Executor()

	if _, err = query.ScanStructContext(ctx, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
