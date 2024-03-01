package app

import (
	"context"
	"fmt"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/sirupsen/logrus"
)

func (a *App) GetUserSummary(ctx context.Context, username string) (*qms.Subscription, error) {
	// Set up the log context.
	log := log.WithFields(
		logrus.Fields{
			"context": "user summary",
			"user":    username,
		},
	)

	// Get the user summary.
	d := db.New(a.db)

	var subscription *db.Subscription
	tx, err := d.Begin()
	if err != nil {
		return nil, err
	}
	err = tx.Wrap(func() error {
		log.Debug("before getting the active user plan")
		subscription, err = d.GetActiveSubscription(ctx, username, db.WithTX(tx))
		if err != nil {
			log.Errorf("unable to get the active user plan: %s", err)
			return err
		}
		log.Debug("after getting the active user plan")

		if subscription == nil || subscription.ID == "" {
			user, err := d.EnsureUser(ctx, username, db.WithTX(tx))
			if err != nil {
				log.Errorf("unable to ensure that the user exists in the database: %s", err)
				return err
			}

			plan, err := d.GetPlanByName(ctx, db.DefaultPlanName, db.WithTX(tx))
			if err != nil {
				log.Errorf("unable to look up the default plan: %s", err)
				return err
			}

			opts := db.DefaultSubscriptionOptions()
			subscriptionID, err := d.SetActiveSubscription(ctx, user.ID, plan, opts, db.WithTX(tx))
			if err != nil {
				log.Errorf("unable to subscribe the user to the default plan: %s", err)
				return err
			}

			subscription, err = d.GetSubscriptionByID(ctx, subscriptionID, db.WithTX(tx))
			if err != nil {
				log.Errorf("unable to look up the new user plan: %s", err)
				return err
			}
			if subscription == nil {
				err = fmt.Errorf("the newly inserted user plan could not be found")
				log.Error(err)
				return err
			}
		}

		log.Debug("before getting the user plan details")
		err = d.LoadSubscriptionDetails(ctx, subscription, db.WithTX(tx))
		if err != nil {
			return err
		}
		log.Debug("affter getting the user plan details")

		return nil
	})
	if err != nil {
		return nil, err
	}

	return subscription.ToQMSSubscription(), nil
}

func (a *App) GetUserSummaryHandler(subject, reply string, request *qms.RequestByUsername) {
	var err error

	log := log.WithFields(logrus.Fields{"context": "user summary"})

	response := pbinit.NewSubscriptionResponse()

	ctx, span := pbinit.InitQMSRequestByUsername(request, subject)
	defer span.End()

	sendError := func(ctx context.Context, response *qms.SubscriptionResponse, err error) {
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

	subscription, err := a.GetUserSummary(ctx, username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	log.Warnf("Reply: %s", reply)

	response.Subscription = subscription
	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
