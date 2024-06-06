package app

import (
	"context"
	"net/http"

	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/labstack/echo/v4"
)

func (a *App) addQuota(ctx context.Context, request *qms.AddQuotaRequest) *qms.QuotaResponse {
	var err error
	response := pbinit.NewQuotaResponse()

	subscriptionID := request.Quota.SubscriptionId

	d := db.New(a.db)

	if err = d.UpsertQuota(ctx, float64(request.Quota.Quota), request.Quota.ResourceType.Uuid, subscriptionID); err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	value, _, err := d.GetCurrentQuota(ctx, request.Quota.ResourceType.Uuid, subscriptionID)
	if err != nil {
		response.Error = errors.NatsError(ctx, err)
		return response
	}

	response.Quota = &qms.Quota{
		Quota:          value,
		ResourceType:   request.Quota.ResourceType,
		SubscriptionId: subscriptionID,
	}

	return response
}

func (a *App) AddQuotaHandler(subject, reply string, request *qms.AddQuotaRequest) {
	var err error

	log := log.WithField("context", "add quota")

	ctx, span := pbinit.InitQMSAddQuotaRequest(request, subject)
	defer span.End()

	response := a.addQuota(ctx, request)

	if response.Error != nil {
		log.Error(response.Error.Message)
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) AddQuotaHTTPHandler(c echo.Context) error {
	var (
		err     error
		request qms.AddQuotaRequest
	)

	ctx := c.Request().Context()

	if err = c.Bind(&request); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"message": "bad request",
		})
	}

	response := a.addQuota(ctx, &request)

	if response.Error != nil {
		return c.JSON(int(response.Error.StatusCode), response)
	}

	return c.JSON(http.StatusOK, response)
}
