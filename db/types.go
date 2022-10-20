package db

import "time"

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
	ID              string          `db:"id" goqu:"defaultifempty"`
	ValueType       string          `db:"value_type"`
	Value           float64         `db:"value"`
	EffectiveDate   time.Time       `db:"effective_date"`
	UpdateOperation UpdateOperation `db:"update_operations"`
	ResourceType    ResourceType    `db:"resource_type"`
	User            User            `db:"users"`
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
