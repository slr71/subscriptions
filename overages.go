package main

import (
	"context"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/sirupsen/logrus"
)

func parseFloat64(floatStr string) (float64, error) {
	d, _, err := apd.New(0, 0).SetString(floatStr)
	if err != nil {
		return 0.0, err
	}

	f, err := d.Float64()
	if err != nil {
		return 0.0, err
	}

	return f, nil
}

func (a *App) GetUserOverages(subject, reply string, request *qms.AllUserOveragesRequest) {
	var err error

	log := log.WithFields(logrus.Fields{"context": "list overages"})

	response := pbinit.NewOverageList()
	ctx, span := pbinit.InitAllUserOveragesRequest(request, subject)
	defer span.End()

	sendError := func(ctx context.Context, response *qms.OverageList, err error) {
		log.Error(err)
		response.Error = natsError(ctx, err)
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

	// If s.ReportOverages is false, then return the empty list of overages that was just created.
	if !a.ReportOverages {
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
		return
	}

	d := db.New(a.db)

	results, err := d.GetUserOverages(ctx, username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	log.Debug("after calling db.GetUserOverages()")

	for _, r := range results {
		quota := r.QuotaValue
		usage := r.UsageValue

		response.Overages = append(response.Overages, &qms.Overage{
			ResourceName: r.ResourceType.Name,
			Quota:        float32(quota),
			Usage:        float32(usage),
		})
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
