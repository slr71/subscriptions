package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/doug-martin/goqu/v9"
)

type GoquDatabase interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	From(cols ...interface{}) *goqu.SelectDataset
	Insert(table interface{}) *goqu.InsertDataset
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ScanStruct(i interface{}, query string, args ...interface{}) (bool, error)
	ScanStructContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error)
	ScanStructs(i interface{}, query string, args ...interface{}) error
	ScanStructsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error
	ScanVal(i interface{}, query string, args ...interface{}) (bool, error)
	ScanValContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error)
	ScanVals(i interface{}, query string, args ...interface{}) error
	ScanValsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error
	Select(cols ...interface{}) *goqu.SelectDataset
	Trace(op, sqlString string, args ...interface{})
	Truncate(table ...interface{}) *goqu.TruncateDataset
	Update(table interface{}) *goqu.UpdateDataset
}

var UpdateOperationNames = []string{"ADD", "SET"}

const UsagesTrackedMetric = "usages"
const QuotasTrackedMetric = "quotas"

type UpdateOperation struct {
	ID   string `db:"id" goqu:"defaultifempty"`
	Name string `db:"name"`
}

type User struct {
	ID       string `db:"id" goqu:"defaultifempty"`
	Username string `db:"username"`
}

type ResourceType struct {
	ID   string `db:"id" goqu:"defaultifempty"`
	Name string `db:"name"`
	Unit string `db:"unit"`
}

var ResourceTypeNames = []string{
	"cpu.hours",
	"data.size",
}

var ResourceTypeUnits = []string{
	"cpu hours",
	"bytes",
}

const (
	UpdateTypeSet = "SET"
	UpdateTypeAdd = "ADD"
)

type Update struct {
	ID                  string    `db:"id" goqu:"defaultifempty"`
	ValueType           string    `db:"value_type"`
	Value               float64   `db:"value"`
	EffectiveDate       time.Time `db:"effective_date"`
	CreatedBy           string    `db:"created_by"`
	CreatedAt           time.Time `db:"created_at" goqu:"defaultifempty"`
	LastModifiedBy      string    `db:"last_modified_by"`
	LastModifiedAt      time.Time `db:"last_modified_at" goqu:"defaultifempty"`
	ResourceTypeID      string    `db:"resource_type_id"`
	ResourceTypeName    string    `db:"resource_type_name"`
	ResourceTypeUnit    string    `db:"resource_type_unit"`
	UserID              string    `db:"user_id"`
	Username            string    `db:"username"`
	UpdateOperationID   string    `db:"update_operation_id"`
	UpdateOperationName string    `db:"update_operation_name"`
}

type UserPlan struct {
	ID                 string    `db:"id" goqu:"defaultifempty"`
	EffectiveStartDate time.Time `db:"effective_start_date"`
	EffectiveEndDate   time.Time `db:"effective_end_date"`
	UserID             string    `db:"user_id"`
	User               User      `db:"users"`
	PlanID             string    `db:"plan_id"`
	CreatedBy          string    `db:"created_by"`
	CreatedAt          time.Time `db:"created_at" goqu:"defaultifempty"`
	LastModifiedBy     string    `db:"last_modified_by"`
	LastModifiedAt     string    `db:"last_modified_at" goqu:"defaultifempty"`
}

type Plan struct {
	ID          *string `gorm:"type:uuid;default:uuid_generate_v1()" json:"id"`
	Name        string  `gorm:"not null;unique" json:"name"`
	Description string  `gorm:"not null" json:"description"`
}

type PlanQuotaDefault struct {
	ID           string       `db:"id" goqu:"defaultifempty"`
	Plan         Plan         `db:"plans"`
	QuotaValue   float64      `db:"quota_value"`
	ResourceType ResourceType `db:"resource_types"`
}

type Usage struct {
	ID           string       `db:"id" goqu:"defaultifempty"`
	Usage        float64      `db:"usages"`
	UserPlan     UserPlan     `db:"user_plans"`
	ResourceType ResourceType `db:"resource_types"`
}

type Quota struct {
	ID           string       `db:"id" goqu:"defaultifempty"`
	Quota        float64      `db:"quota"`
	ResourceType ResourceType `db:"resource_types"`
	UserPlan     UserPlan     `db:"user_plans"`
}
