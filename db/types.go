package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/cyverse-de/p/go/qms"
	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
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

type SQLStatement interface {
	ToSQL() (sql string, params []interface{}, err error)
}

var CurrentTimestamp = goqu.L("CURRENT_TIMESTAMP")

var UpdateOperationNames = []string{"ADD", "SET"}

const UsagesTrackedMetric = "usages"
const QuotasTrackedMetric = "quotas"

const DefaultPlanName = "Basic"

type UpdateOperation struct {
	ID   string `db:"id" goqu:"defaultifempty"`
	Name string `db:"name"`
}

type User struct {
	ID       string `db:"id" goqu:"defaultifempty"`
	Username string `db:"username"`
}

func (u User) ToQMSUser() *qms.QMSUser {
	return &qms.QMSUser{
		Uuid:     u.ID,
		Username: u.Username,
	}
}

type ResourceType struct {
	ID   string `db:"id" goqu:"defaultifempty"`
	Name string `db:"name"`
	Unit string `db:"unit"`
}

func (rt ResourceType) ToQMSResourceType() *qms.ResourceType {
	return &qms.ResourceType{
		Uuid: rt.ID,
		Name: rt.Name,
		Unit: rt.Unit,
	}
}

func NewResourceTypeFromQMS(q *qms.ResourceType) *ResourceType {
	return &ResourceType{
		ID:   q.Uuid,
		Name: q.Name,
		Unit: q.Unit,
	}
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
	CreatedBy       string          `db:"created_by"`
	CreatedAt       time.Time       `db:"created_at" goqu:"defaultifempty"`
	LastModifiedBy  string          `db:"last_modified_by"`
	LastModifiedAt  time.Time       `db:"last_modified_at" goqu:"defaultifempty"`
	ResourceType    ResourceType    `db:"resource_types"`
	User            User            `db:"users"`
	UpdateOperation UpdateOperation `db:"update_operations"`
}

type Subscription struct {
	ID                 string    `db:"id" goqu:"defaultifempty"`
	EffectiveStartDate time.Time `db:"effective_start_date"`
	EffectiveEndDate   time.Time `db:"effective_end_date"`
	User               User      `db:"users"`
	Plan               Plan      `db:"plans"`
	Quotas             []Quota   `db:"-"`
	Usages             []Usage   `db:"-"`
	CreatedBy          string    `db:"created_by"`
	CreatedAt          time.Time `db:"created_at" goqu:"defaultifempty"`
	LastModifiedBy     string    `db:"last_modified_by"`
	LastModifiedAt     string    `db:"last_modified_at" goqu:"defaultifempty"`
	Paid               bool      `db:"paid" goqu:"defaultifempty"`
}

func NewSubscriptionFromQMS(s *qms.Subscription) *Subscription {
	var quotas []Quota
	var usages []Usage

	for _, sq := range s.Quotas {
		quotas = append(quotas, *NewQuotaFromQMS(sq))
	}
	for _, su := range s.Usages {
		usages = append(usages, *NewUsageFromQMS(su))
	}

	return &Subscription{
		ID:                 s.Uuid,
		EffectiveStartDate: s.EffectiveEndDate.AsTime(),
		EffectiveEndDate:   s.EffectiveEndDate.AsTime(),
		User: User{
			ID:       s.User.Uuid,
			Username: s.User.Username,
		},
		Quotas:         quotas,
		Usages:         usages,
		Plan:           *NewPlanFromQMS(s.Plan),
		CreatedBy:      s.User.Username,
		LastModifiedBy: s.User.Username,
		Paid:           s.Paid,
	}
}

func (up Subscription) ToQMSSubscription() *qms.Subscription {
	// Convert the list of quotas.
	quotas := make([]*qms.Quota, len(up.Quotas))
	for i, quota := range up.Quotas {
		quotas[i] = quota.ToQMSQuota()
	}

	// Convert th elist of usages.
	usages := make([]*qms.Usage, len(up.Usages))
	for i, usage := range up.Usages {
		usages[i] = usage.ToQMSUsage()
	}

	return &qms.Subscription{
		Uuid:               up.ID,
		EffectiveStartDate: timestamppb.New(up.EffectiveStartDate),
		EffectiveEndDate:   timestamppb.New(up.EffectiveEndDate),
		User:               up.User.ToQMSUser(),
		Plan:               up.Plan.ToQMSPlan(),
		Quotas:             quotas,
		Usages:             usages,
		Paid:               up.Paid,
	}
}

type Plan struct {
	ID            string             `db:"id" goqu:"defaultifempty"`
	Name          string             `db:"name"`
	Description   string             `db:"description"`
	QuotaDefaults []PlanQuotaDefault `db:"-"`
}

func NewPlanFromQMS(q *qms.Plan) *Plan {
	pqd := make([]PlanQuotaDefault, 0)
	for _, qd := range q.PlanQuotaDefaults {
		pqd = append(pqd, *NewPlanQuotaDefaultFromQMS(qd, q.Uuid))
	}
	return &Plan{
		ID:            q.Uuid,
		Name:          q.Name,
		Description:   q.Description,
		QuotaDefaults: pqd,
	}
}

func (p Plan) ToQMSPlan() *qms.Plan {
	// Convert the quota defaults.
	quotaDefaults := make([]*qms.QuotaDefault, len(p.QuotaDefaults))
	for i, quotaDefault := range p.QuotaDefaults {
		quotaDefaults[i] = quotaDefault.ToQMSQuotaDefault()
	}

	return &qms.Plan{
		Uuid:              p.ID,
		Name:              p.Name,
		Description:       p.Description,
		PlanQuotaDefaults: quotaDefaults,
	}
}

type PlanQuotaDefault struct {
	ID           string       `db:"id" goqu:"defaultifempty"`
	PlanID       string       `db:"plan_id"`
	QuotaValue   float64      `db:"quota_value"`
	ResourceType ResourceType `db:"resource_types"`
}

func NewPlanQuotaDefaultFromQMS(q *qms.QuotaDefault, planID string) *PlanQuotaDefault {
	return &PlanQuotaDefault{
		ID:           q.Uuid,
		PlanID:       planID,
		QuotaValue:   q.QuotaValue,
		ResourceType: *NewResourceTypeFromQMS(q.ResourceType),
	}
}

func (pqd PlanQuotaDefault) ToQMSQuotaDefault() *qms.QuotaDefault {
	return &qms.QuotaDefault{
		Uuid:         pqd.ID,
		QuotaValue:   pqd.QuotaValue,
		ResourceType: pqd.ResourceType.ToQMSResourceType(),
	}
}

type Usage struct {
	ID             string       `db:"id" goqu:"defaultifempty"`
	Usage          float64      `db:"usage"`
	SubscriptionID string       `db:"subscription_id"`
	ResourceType   ResourceType `db:"resource_types"`
	CreatedBy      string       `db:"created_by"`
	CreatedAt      time.Time    `db:"created_at"`
	LastModifiedBy string       `db:"last_modified_by"`
	LastModifiedAt time.Time    `db:"last_modified_at"`
}

func NewUsageFromQMS(q *qms.Usage) *Usage {
	return &Usage{
		ID:             q.Uuid,
		Usage:          q.Usage,
		SubscriptionID: q.SubscriptionId,
		ResourceType:   *NewResourceTypeFromQMS(q.ResourceType),
		CreatedBy:      q.CreatedBy,
		CreatedAt:      q.CreatedAt.AsTime(),
		LastModifiedBy: q.LastModifiedBy,
		LastModifiedAt: q.LastModifiedAt.AsTime(),
	}
}

func (u Usage) ToQMSUsage() *qms.Usage {
	return &qms.Usage{
		Uuid:           u.ID,
		Usage:          u.Usage,
		ResourceType:   u.ResourceType.ToQMSResourceType(),
		CreatedBy:      u.CreatedBy,
		CreatedAt:      timestamppb.New(u.CreatedAt),
		LastModifiedBy: u.LastModifiedBy,
		LastModifiedAt: timestamppb.New(u.LastModifiedAt),
	}
}

type Quota struct {
	ID             string       `db:"id" goqu:"defaultifempty"`
	Quota          float64      `db:"quota"`
	ResourceType   ResourceType `db:"resource_types"`
	Subscription   Subscription `db:"subscriptions"`
	CreatedBy      string       `db:"created_by"`
	CreatedAt      time.Time    `db:"created_at"`
	LastModifiedBy string       `db:"last_modified_by"`
	LastModifiedAt time.Time    `db:"last_modified_at"`
}

func NewQuotaFromQMS(q *qms.Quota) *Quota {
	return &Quota{
		ID:           q.Uuid,
		Quota:        q.Quota,
		ResourceType: *NewResourceTypeFromQMS(q.ResourceType),
		Subscription: Subscription{
			ID: q.SubscriptionId,
		},
		CreatedBy:      q.CreatedBy,
		CreatedAt:      q.CreatedAt.AsTime(),
		LastModifiedBy: q.LastModifiedBy,
		LastModifiedAt: q.LastModifiedAt.AsTime(),
	}
}

func (q Quota) ToQMSQuota() *qms.Quota {
	return &qms.Quota{
		Uuid:           q.ID,
		Quota:          q.Quota,
		ResourceType:   q.ResourceType.ToQMSResourceType(),
		CreatedBy:      q.CreatedBy,
		CreatedAt:      timestamppb.New(q.CreatedAt),
		LastModifiedBy: q.LastModifiedBy,
		LastModifiedAt: timestamppb.New(q.LastModifiedAt),
	}
}

type Overage struct {
	SubscriptionID string       `db:"subscription_id"`
	User           User         `db:"users"`
	Plan           Plan         `db:"plans"`
	ResourceType   ResourceType `db:"resource_types"`
	QuotaValue     float64      `db:"quota_value"`
	UsageValue     float64      `db:"usage_value"`
}

type Addon struct {
	ID            string       `db:"id" goqu:"defaultifempty,skipupdate"`
	Name          string       `db:"name"`
	Description   string       `db:"description"`
	ResourceType  ResourceType `db:"resource_types"`
	DefaultAmount float64      `db:"default_amount"`
	DefaultPaid   bool         `db:"default_paid"`
}

func NewAddonFromQMS(q *qms.Addon) *Addon {
	return &Addon{
		Name:          q.Name,
		Description:   q.Description,
		DefaultAmount: q.DefaultAmount,
		DefaultPaid:   q.DefaultPaid,
		ResourceType:  *NewResourceTypeFromQMS(q.ResourceType),
	}
}

func (a *Addon) ToQMSType() *qms.Addon {
	return &qms.Addon{
		Uuid:          a.ID,
		Name:          a.Name,
		Description:   a.Description,
		DefaultAmount: a.DefaultAmount,
		DefaultPaid:   a.DefaultPaid,
		ResourceType: &qms.ResourceType{
			Uuid: a.ResourceType.ID,
			Name: a.ResourceType.Name,
			Unit: a.ResourceType.Unit,
		},
	}
}

type UpdateAddon struct {
	ID                  string  `db:"id" goqu:"skipupdate"`
	Name                string  `db:"name"`
	UpdateName          bool    `db:"-"`
	Description         string  `db:"description"`
	UpdateDescription   bool    `db:"-"`
	ResourceTypeID      string  `db:"resource_type_id"`
	UpdateResourceType  bool    `db:"-"`
	DefaultAmount       float64 `db:"default_amount"`
	UpdateDefaultAmount bool    `db:"-"`
	DefaultPaid         bool    `db:"default_paid"`
	UpdateDefaultPaid   bool    `db:"-"`
}

func NewUpdateAddonFromQMS(u *qms.UpdateAddonRequest) *UpdateAddon {
	update := &UpdateAddon{
		ID:                  u.Addon.Uuid,
		UpdateName:          u.UpdateName,
		UpdateDescription:   u.UpdateDescription,
		UpdateResourceType:  u.UpdateResourceType,
		UpdateDefaultAmount: u.UpdateDefaultAmount,
		UpdateDefaultPaid:   u.UpdateDefaultPaid,
	}

	if update.UpdateName {
		update.Name = u.Addon.Name
	}
	if update.UpdateDescription {
		update.Description = u.Addon.Description
	}
	if update.UpdateDefaultAmount {
		update.DefaultAmount = float64(u.Addon.DefaultAmount)
	}
	if update.UpdateDefaultPaid {
		update.DefaultPaid = u.Addon.DefaultPaid
	}
	if update.UpdateResourceType {
		update.ResourceTypeID = u.Addon.ResourceType.Uuid
	}
	return update
}

type SubscriptionAddon struct {
	ID           string       `db:"id" goqu:"defaultifempty,skipupdate"`
	Addon        Addon        `db:"addons"`
	Subscription Subscription `db:"subscriptions"`
	Amount       float64      `db:"amount"`
	Paid         bool         `db:"paid"`
}

func NewSubscriptionAddonFromQMS(sa *qms.SubscriptionAddon) *SubscriptionAddon {
	return &SubscriptionAddon{
		ID:           sa.Uuid,
		Addon:        *NewAddonFromQMS(sa.Addon),
		Subscription: *NewSubscriptionFromQMS(sa.Subscription),
		Amount:       float64(sa.Amount),
		Paid:         sa.Paid,
	}
}

func (sa *SubscriptionAddon) ToQMSType() *qms.SubscriptionAddon {
	return &qms.SubscriptionAddon{
		Uuid:         sa.ID,
		Addon:        sa.Addon.ToQMSType(),
		Subscription: sa.Subscription.ToQMSSubscription(),
		Amount:       sa.Amount,
		Paid:         sa.Paid,
	}
}

type UpdateSubscriptionAddon struct {
	ID                   string  `db:"id" goqu:"skipupdate"`
	AddonID              string  `db:"addon_id"`
	UpdateAddonID        bool    `db:"-"`
	SubscriptionID       string  `db:"subscription_id"`
	UpdateSubscriptionID bool    `db:"-"`
	Amount               float64 `db:"amount"`
	UpdateAmount         bool    `db:"-"`
	Paid                 bool    `db:"paid"`
	UpdatePaid           bool    `db:"-"`
}

// NewUpdateSubscriptoinAddonFromQMS does what the name implies. A caveat is that
// The UpdateAddonID and UpdateSubscriptionID fields are ignored by the logic in
// the service.
func NewUpdateSubscriptionAddonFromQMS(q *qms.UpdateSubscriptionAddonRequest) *UpdateSubscriptionAddon {
	update := &UpdateSubscriptionAddon{
		ID:                   q.SubscriptionAddon.Uuid,
		UpdateAddonID:        q.UpdateAddonId,
		UpdateSubscriptionID: q.UpdateSubscriptionId,
		UpdateAmount:         q.UpdateAmount,
		UpdatePaid:           q.UpdatePaid,
	}
	if update.UpdateAmount {
		update.Amount = float64(q.SubscriptionAddon.Amount)
	}
	if update.UpdatePaid {
		update.Paid = q.SubscriptionAddon.Paid
	}
	return update
}
