package app

import (
	"context"

	"errors"

	serrors "github.com/cyverse-de/subscriptions/errors"

	qmsinit "github.com/cyverse-de/go-mod/pbinit/qms"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
)

func (a *App) AddAddonHandler(subject, reply string, request *qms.AddAddonRequest) {
	var err error

	log := log.WithField("context", "adding new available addon")

	response := qmsinit.NewAddonResponse()

	sendError := func(ctx context.Context, response *qms.AddonResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	ctx, span := qmsinit.InitAddAddonRequest(request, subject)
	defer span.End()

	d := db.New(a.db)

	reqAddon := request.Addon

	if reqAddon.Name == "" {
		sendError(ctx, response, errors.New("name must be set"))
		return
	}

	if reqAddon.Description == "" {
		sendError(ctx, response, errors.New("descriptions must be set"))
		return
	}

	if reqAddon.DefaultAmount <= 0.0 {
		sendError(ctx, response, errors.New("default_amount must be greater than 0.0"))
		return
	}

	if reqAddon.ResourceType.Name == "" && reqAddon.ResourceType.Uuid == "" {
		sendError(ctx, response, errors.New("resource_type.name or resource_type.uuid must be set"))
		return
	}

	var lookupRT *db.ResourceType

	if reqAddon.ResourceType.Name != "" && reqAddon.ResourceType.Uuid == "" {
		lookupRT, err = d.GetResourceTypeByName(ctx, reqAddon.ResourceType.Name)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
	} else {
		lookupRT, err = d.GetResourceType(ctx, reqAddon.ResourceType.Uuid)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
	}

	newAddon := db.NewAddonFromQMS(request.Addon)
	newAddon.ResourceType = *lookupRT

	newID, err := d.AddAddon(ctx, newAddon)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Addon = newAddon.ToQMSType()
	response.Addon.Uuid = newID

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) ListAddonsHandler(subject, reply string, request *qms.NoParamsRequest) {
	var err error

	log := log.WithField("context", "list addons")

	response := qmsinit.NewAddonListResponse()

	sendError := func(ctx context.Context, response *qms.AddonListResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	ctx, span := qmsinit.InitNoParamsRequest(request, subject)
	defer span.End()

	d := db.New(a.db)

	results, err := d.ListAddons(ctx)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	for _, addon := range results {
		response.Addons = append(response.Addons, addon.ToQMSType())
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
