package app

import (
	"context"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/cyverse-de/subscriptions/utils"
	"github.com/sirupsen/logrus"
)

func (a *App) AddUserHandler(subject, reply string, request *qms.AddUserRequest) {
	var (
		err error
	)

	log := log.WithField("context", "add user")

	ctx, span := pbinit.InitQMSAddUserRequest(request, subject)
	defer span.End()

	response := pbinit.NewQMSAddUserResponse()

	sendError := func(ctx context.Context, response *qms.AddUserResponse, err error) {
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

	opts, err := utils.OptsForValues(request.Paid, request.Periods, request.EndDate)
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	log = log.WithFields(
		logrus.Fields{
			"user":     username,
			"plan":     request.PlanName,
			"paid":     opts.Paid,
			"periods":  opts.Periods,
			"end_date": opts.EndDate,
		},
	)

	tx, err := d.Begin()
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// extract information about the subscription from the request
	planName := request.PlanName

	plan, err := d.GetPlanByName(ctx, planName, db.WithTX(tx))
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	// look for an existing user.
	userExists, err := d.UserExists(ctx, username, db.WithTX(tx))
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	var userID string

	// add the user.
	if !userExists {
		userID, err = d.AddUser(ctx, username, db.WithTX(tx))
		if err != nil {
			sendError(ctx, response, err)
			return
		}
	} else {
		userID, err = d.GetUserID(ctx, username, db.WithTX(tx))
		if err != nil {
			sendError(ctx, response, err)
			return
		}
	}

	// Create a new subscription if the caller requested it.
	createSubscription := request.Force

	// Also create a new subscription if the user doesn't have one yet.
	if !createSubscription {
		hasPlan, err := d.UserHasActivePlan(ctx, username, db.WithTX(tx))
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		createSubscription = !hasPlan
	}

	// Also create a new subscription if the user's current subscription plan doesn't match the requested one.
	if !createSubscription {
		onPlan, err := d.UserOnPlan(ctx, username, plan.Name, db.WithTX(tx))
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		createSubscription = !onPlan
	}

	// Create the subscription if we're supposed to.
	if createSubscription {
		if _, err = d.SetActiveSubscription(ctx, userID, plan, opts, db.WithTX(tx)); err != nil {
			sendError(ctx, response, err)
			return
		}
	}

	// Commit all of the changes
	if err = tx.Commit(); err != nil {
		sendError(ctx, response, err)
		return
	}

	response.PlanName = plan.Name
	response.PlanUuid = plan.ID
	response.Username = username
	response.Uuid = userID

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
