package app

import (
	"context"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
)

func (a *App) AddQuotaHandler(subject, reply string, request *qms.AddQuotaRequest) {
	var err error

	log := log.WithField("context", "add quota")

	sendError := func(ctx context.Context, response *qms.QuotaResponse, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	response := pbinit.NewQuotaResponse()

	ctx, span := pbinit.InitQMSAddQuotaRequest(request, subject)
	defer span.End()

	userPlanID := request.Quota.UserPlanId

	d := db.New(a.db)

	_, update, err := d.GetCurrentQuota(ctx, request.Quota.ResourceType.Uuid, userPlanID)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	if err = d.UpsertQuota(ctx, update, float64(request.Quota.Quota), request.Quota.ResourceType.Uuid, userPlanID); err != nil {
		sendError(ctx, response, err)
		return
	}

	value, _, err := d.GetCurrentQuota(ctx, request.Quota.ResourceType.Uuid, userPlanID)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Quota.Quota = float32(value)
	response.Quota.ResourceType = request.Quota.ResourceType
	response.Quota.UserPlanId = userPlanID

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
