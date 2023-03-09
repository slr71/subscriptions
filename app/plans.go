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

func (a *App) sendPlanResponseError(reply string, log *logrus.Entry) func(context.Context, *qms.PlanResponse, error) {
	return func(ctx context.Context, response *qms.PlanResponse, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}
}

func (a *App) ListPlansHandler(subject, reply string, request *qms.NoParamsRequest) {
	log := log.WithField("context", "list plans")

	sendError := func(ctx context.Context, response *qms.PlanList, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	response := pbinit.NewPlanList()
	ctx, span := pbinit.InitQMSNoParamsRequest(request, subject)
	defer span.End()

	d := db.New(a.db)
	plans, err := d.ListPlans(ctx)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	for _, p := range plans {
		newP := &qms.Plan{
			Uuid:              p.ID,
			Name:              p.Name,
			Description:       p.Description,
			PlanQuotaDefaults: []*qms.QuotaDefault{},
		}

		for _, q := range p.QuotaDefaults {
			newP.PlanQuotaDefaults = append(newP.PlanQuotaDefaults, &qms.QuotaDefault{
				Uuid:       q.ID,
				QuotaValue: q.QuotaValue,
				ResourceType: &qms.ResourceType{
					Uuid: q.ResourceType.ID,
					Name: q.ResourceType.Name,
					Unit: q.ResourceType.Unit,
				},
			})
		}

		response.Plans = append(response.Plans, newP)
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) AddPlanHandler(subject, reply string, request *qms.AddPlanRequest) {
	log := log.WithField("context", "list plans")

	sendError := a.sendPlanResponseError(reply, log)

	response := pbinit.NewPlanResponse()
	ctx, span := pbinit.InitQMSAddPlanRequest(request, subject)
	defer span.End()

	d := db.New(a.db)

	var qd []db.PlanQuotaDefault
	var newPlanID string
	tx, err := d.Begin()
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	err = tx.Wrap(func() error {
		var err error

		for _, pqd := range request.Plan.PlanQuotaDefaults {
			qd = append(qd, db.PlanQuotaDefault{
				QuotaValue: float64(pqd.QuotaValue),
				ResourceType: db.ResourceType{
					ID:   pqd.ResourceType.Uuid,
					Name: pqd.ResourceType.Name,
					Unit: pqd.ResourceType.Unit,
				},
			})
		}

		newPlanID, err = d.AddPlan(ctx, &db.Plan{
			Name:          request.Plan.Name,
			Description:   request.Plan.Description,
			QuotaDefaults: qd,
		}, db.WithTX(tx))

		return err
	})
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Plan = request.Plan
	response.Plan.Uuid = newPlanID

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) GetPlanHandler(subject, reply string, request *qms.PlanRequest) {
	log := log.WithField("context", "get plan")

	sendError := a.sendPlanResponseError(reply, log)

	response := pbinit.NewPlanResponse()
	ctx, span := pbinit.InitQMSPlanRequest(request, subject)
	defer span.End()

	d := db.New(a.db)

	plan, err := d.GetPlanByID(ctx, request.PlanId)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Plan = &qms.Plan{
		Uuid:              plan.ID,
		Name:              plan.Name,
		Description:       plan.Description,
		PlanQuotaDefaults: []*qms.QuotaDefault{},
	}

	for _, q := range plan.QuotaDefaults {
		response.Plan.PlanQuotaDefaults = append(
			response.Plan.PlanQuotaDefaults,
			&qms.QuotaDefault{
				Uuid:       q.ID,
				QuotaValue: q.QuotaValue,
				ResourceType: &qms.ResourceType{
					Uuid: q.ResourceType.ID,
					Name: q.ResourceType.Name,
					Unit: q.ResourceType.Unit,
				},
			})
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) UpsertQuotaDefaultsHandler(subject, reply string, request *qms.AddPlanQuotaDefaultRequest) {
	sendError := func(ctx context.Context, response *qms.QuotaDefaultResponse, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}
	response := pbinit.NewQuotaDefaultResponse()
	ctx, span := pbinit.InitQMSAddPlanQuotaDefaultRequest(request, subject)
	defer span.End()
	sendError(ctx, response, fmt.Errorf("not implemented"))
}
