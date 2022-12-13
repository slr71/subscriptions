package app

import (
	"context"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (a *App) GetUserSummary(subject, reply string, request *qms.RequestByUsername) {
	var (
		err  error
		pqds []*qms.QuotaDefault
		qs   []*qms.Quota
		us   []*qms.Usage
	)

	log := log.WithFields(logrus.Fields{"context": "user summary"})

	response := pbinit.NewUserPlanResponse()

	ctx, span := pbinit.InitQMSRequestByUsername(request, subject)
	defer span.End()

	sendError := func(ctx context.Context, response *qms.UserPlanResponse, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	username, err := a.FixUsername(request.Username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	log = log.WithFields(logrus.Fields{"user": username})

	d := db.New(a.db)

	log.Debug("before getting the active user plan")
	userPlan, err := d.GetActiveUserPlan(ctx, username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	log.Debug("after getting the active user plan")

	log.Debug("before getting the user plan details")
	defaults, quotas, usages, err := d.UserPlanDetails(ctx, userPlan)
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	log.Debug("affter getting the user plan details")

	for _, pqd := range defaults {
		def := &qms.QuotaDefault{
			Uuid:       pqd.ID,
			QuotaValue: float32(pqd.QuotaValue),
			ResourceType: &qms.ResourceType{
				Uuid: pqd.ResourceType.ID,
				Name: pqd.ResourceType.Name,
				Unit: pqd.ResourceType.Unit,
			},
		}
		pqds = append(pqds, def)
	}

	for _, quota := range quotas {
		q := &qms.Quota{
			Uuid:  quota.ID,
			Quota: float32(quota.Quota),
			ResourceType: &qms.ResourceType{
				Uuid: quota.ResourceType.ID,
				Name: quota.ResourceType.Name,
				Unit: quota.ResourceType.Unit,
			},
			CreatedBy:      quota.CreatedBy,
			CreatedAt:      timestamppb.New(quota.CreatedAt),
			LastModifiedBy: quota.LastModifiedBy,
			LastModifiedAt: timestamppb.New(quota.LastModifiedAt),
		}
		qs = append(qs, q)
	}

	for _, usage := range usages {
		u := &qms.Usage{
			Uuid:       usage.ID,
			Usage:      usage.Usage,
			UserPlanId: userPlan.ID,
			ResourceType: &qms.ResourceType{
				Uuid: usage.ResourceType.ID,
				Name: usage.ResourceType.Name,
				Unit: usage.ResourceType.Unit,
			},
			CreatedBy:      usage.CreatedBy,
			CreatedAt:      timestamppb.New(usage.CreatedAt),
			LastModifiedBy: usage.LastModifiedBy,
			LastModifiedAt: timestamppb.New(usage.LastModifiedAt),
		}
		us = append(us, u)
	}

	response.UserPlan = &qms.UserPlan{
		Uuid:               userPlan.ID,
		EffectiveStartDate: timestamppb.New(userPlan.EffectiveStartDate),
		EffectiveEndDate:   timestamppb.New(userPlan.EffectiveEndDate),
		User: &qms.QMSUser{
			Uuid:     userPlan.User.ID,
			Username: userPlan.User.Username,
		},
		Plan: &qms.Plan{
			Uuid:              userPlan.Plan.ID,
			Name:              userPlan.Plan.Name,
			Description:       userPlan.Plan.Description,
			PlanQuotaDefaults: pqds,
		},
		Quotas: qs,
		Usages: us,
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
