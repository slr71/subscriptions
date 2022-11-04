package main

import (
	"context"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/sirupsen/logrus"
)

func (a *App) GetUsagesHandler(subject, reply string, request *qms.GetUsages) {
	var err error

	log := log.WithFields(logrus.Fields{"context": "getting usages"})
	response := pbinit.NewUsageList()
	ctx, span := pbinit.InitGetUsages(request, subject)
	defer span.End()

	sendError := func(ctx context.Context, response *qms.UsageList, err error) {
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

	userPlan, err := d.GetActiveUserPlan(ctx, username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	usages, err := d.UserPlanUsages(ctx, userPlan.ID)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	for _, usage := range usages {
		response.Usages = append(response.Usages, &qms.Usage{
			Uuid:       usage.ID,
			Usage:      usage.Usage,
			UserPlanId: userPlan.ID,
			ResourceType: &qms.ResourceType{
				Uuid: usage.ResourceType.ID,
				Name: usage.ResourceType.Name,
				Unit: usage.ResourceType.Unit,
			},
		})
	}

	log.Info("successfully found usages")

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) AddUsageHandler(subject, reply string, request *qms.AddUsage) {
	var (
		err   error
		usage db.Usage
	)

	log := log.WithFields(logrus.Fields{"context": "adding usage information"})

	log.Debugf("subject: %s; reply: %s", subject, reply)

	response := pbinit.NewUsageResponse()
	ctx, span := pbinit.InitAddUsage(request, subject)
	defer span.End()

	sendError := func(ctx context.Context, response *qms.UsageResponse, err error) {
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

	d := db.New(a.db)

	userPlan, err := d.GetActiveUserPlan(ctx, username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	// Validate update type.
	if _, err = d.GetOperationID(ctx, request.UpdateType); err != nil {
		sendError(ctx, response, err)
		return
	}

	usage = db.Usage{
		Usage:      request.UsageValue,
		UserPlanID: userPlan.ID,
		ResourceType: db.ResourceType{
			Name: request.ResourceName,
			Unit: request.ResourceUnit,
		},
	}

	if err = d.AddUsage(ctx, request.UpdateType, &usage); err != nil {
		sendError(ctx, response, err)
		return
	}
}
