package utils

import (
	"fmt"
	"regexp"
	"time"

	"github.com/cyverse-de/subscriptions/db"
)

const (
	DateOnly      = time.DateOnly
	DateTimeLocal = "2006-01-02T15:04:05"
	RFC3339       = time.RFC3339
)

var (
	DateOnlyRegexp      = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	DateTimeLocalRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$`)
	RFC3339Regexp       = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`)
)

// layoutForValue returns the layout to use for a given timestamp value.
func layoutForValue(value string) (string, error) {
	switch {
	case DateOnlyRegexp.MatchString(value):
		return DateOnly, nil
	case DateTimeLocalRegexp.MatchString(value):
		return DateTimeLocal, nil
	case RFC3339Regexp.MatchString(value):
		return RFC3339, nil
	default:
		return "", fmt.Errorf("unrecognized timestamp layout: %s", value)
	}
}

// Parse attempts to parse the given value as a timestamp. The timestamp will be parsed in the time zone of the
// current location unless the time zone is included in the timestamp itself. The accepted formats are:
//
//	2024-02-21                - Midnight on the specified date in the local time zone.
//	2024-02-21T01:02:03       - The specified date and time in the local time zone.
//	2024-02-21T01:02:03Z      - The specified date and time in UTC.
//	2024-02-01T01:02:03-07:00 - The specified date and time in the specified time zone.
func ParseTimestamp(value string) (time.Time, error) {
	var t time.Time

	// Determine the timestamp layout.
	layout, err := layoutForValue(value)
	if err != nil {
		return t, err
	}

	// Parse the timestamp.
	t, err = time.ParseInLocation(layout, value, time.Now().Location())
	return t, err
}

// EndTimeForValue returns the time to use for the given date value. If the given date value is empty then the
// resulting timestamp will be one year from the current time. Otherwise, the timestamp will be parsed using
// ParseTimestamp.
func EndTimeForValue(value string) (time.Time, error) {
	var t time.Time

	// Use the default end time if the value is empty.
	if value == "" {
		return time.Now().AddDate(1, 0, 0), nil
	}

	// Parse the timestamp.
	t, err := ParseTimestamp(value)
	if err != nil {
		return t, err
	}

	// Return an error if the time is in the past.
	if t.Before(time.Now()) {
		return t, fmt.Errorf("the end date must be in the future")
	}

	return t, nil
}

// PeriodsForRequestValue returns the number of periods from the request. If the number of periods is zero then the
// default number of periods (1) is returned. If the number of periods is negative, then an error is returned.
// Otherwise, the select number is returned.
func PeriodsForRequestValue(value int32) (int32, error) {
	// Return the default value if the selected value is zero.
	if value == 0 {
		return 1, nil
	}

	// Return an error if the selected value is negative.
	if value < 0 {
		return 0, fmt.Errorf("the number of periods must be greater than zero")
	}

	return value, nil
}

// OptsForValues returns subscription options for a set of request values.
func OptsForValues(paid bool, periodsVal int32, endTimeVal string) (*db.SubscriptionOptions, error) {
	// Vaidate the periods.
	periods, err := PeriodsForRequestValue(periodsVal)
	if err != nil {
		return nil, err
	}

	// Parse and validate the end time.
	endTime, err := EndTimeForValue(endTimeVal)
	if err != nil {
		return nil, err
	}

	return &db.SubscriptionOptions{
		Paid:    paid,
		Periods: periods,
		EndDate: endTime,
	}, nil
}
