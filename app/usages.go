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

func (a *App) getUsages(ctx context.Context, request *qms.GetUsages) *qms.UsageList {
	response := pbinit.NewUsageList()

	username, err := a.FixUsername(request.Username)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	d := db.New(a.db)

	subscription, err := d.GetActiveSubscription(ctx, username)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	usages, err := d.SubscriptionUsages(ctx, subscription.ID)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	for _, usage := range usages {
		response.Usages = append(response.Usages, &qms.Usage{
			Uuid:           usage.ID,
			Usage:          usage.Usage,
			SubscriptionId: subscription.ID,
			ResourceType: &qms.ResourceType{
				Uuid: usage.ResourceType.ID,
				Name: usage.ResourceType.Name,
				Unit: usage.ResourceType.Unit,
			},
			CreatedAt:      timestamppb.New(usage.CreatedAt),
			CreatedBy:      usage.CreatedBy,
			LastModifiedBy: usage.LastModifiedBy,
			LastModifiedAt: timestamppb.New(usage.LastModifiedAt),
		})
	}

	return response
}

func (a *App) GetUsagesHandler(subject, reply string, request *qms.GetUsages) {
	var err error

	log := log.WithFields(logrus.Fields{"context": "getting usages"})

	ctx, span := pbinit.InitGetUsages(request, subject)
	defer span.End()

	response := a.getUsages(ctx, request)

	if response.Error != nil {
		log.Error(response.Error.Message)
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) addUsage(ctx context.Context, request *qms.AddUsage) *qms.UsageResponse {
	var (
		err   error
		usage db.Usage
	)

	response := pbinit.NewUsageResponse()
	username, err := a.FixUsername(request.Username)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	d := db.New(a.db)

	subscription, err := d.GetActiveSubscription(ctx, username)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	// Validate update type.
	if _, err = d.GetOperationID(ctx, request.UpdateType); err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	resourceID, err := d.GetResourceTypeID(ctx, request.ResourceName, request.ResourceUnit)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	usage = db.Usage{
		Usage:          request.UsageValue,
		SubscriptionID: subscription.ID,
		ResourceType: db.ResourceType{
			ID:   resourceID,
			Name: request.ResourceName,
			Unit: request.ResourceUnit,
		},
	}

	if err = d.CalculateUsage(ctx, request.UpdateType, &usage); err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	u, _, err := d.GetCurrentUsage(ctx, resourceID, subscription.ID)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	response.Usage = &qms.Usage{
		Usage:          u,
		SubscriptionId: subscription.ID,
		ResourceType: &qms.ResourceType{
			Uuid: resourceID,
			Name: request.ResourceName,
			Unit: request.ResourceUnit,
		},
	}

	return response
}

func (a *App) AddUsageHandler(subject, reply string, request *qms.AddUsage) {
	var err error

	ctx, span := pbinit.InitAddUsage(request, subject)
	defer span.End()

	response := a.addUsage(ctx, request)

	if response.Error != nil {
		log.Error(response.Error.Message)
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
