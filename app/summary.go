package app

import (
	"context"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/sirupsen/logrus"
)

func (a *App) GetUserSummary(subject, reply string, request *qms.RequestByUsername) {
	var err error

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

	var userPlan *db.UserPlan
	tx, err := d.Begin()
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	err = tx.Wrap(func() error {
		log.Debug("before getting the active user plan")
		userPlan, err = d.GetActiveUserPlan(ctx, username)
		if err != nil {
			return err
		}
		log.Debug("after getting the active user plan")

		log.Debug("before getting the user plan details")
		err = d.LoadUserPlanDetails(ctx, userPlan)
		if err != nil {
			return err
		}
		log.Debug("affter getting the user plan details")

		return nil
	})
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.UserPlan = userPlan.ToQMSUserPlan()
	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
