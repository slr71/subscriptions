package app

import (
	"context"

	"errors"

	serrors "github.com/cyverse-de/subscriptions/errors"
	"github.com/sirupsen/logrus"

	qmsinit "github.com/cyverse-de/go-mod/pbinit/qms"
	reqinit "github.com/cyverse-de/go-mod/pbinit/requests"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/requests"
	"github.com/cyverse-de/subscriptions/db"
)

func (a *App) sendAddonResponseError(reply string, log *logrus.Entry) func(context.Context, *qms.AddonResponse, error) {
	return func(ctx context.Context, response *qms.AddonResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}
}

func (a *App) sendAddonListResponseError(reply string, log *logrus.Entry) func(context.Context, *qms.AddonListResponse, error) {
	return func(ctx context.Context, response *qms.AddonListResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}
}

func (a *App) sendSubscriptionAddonListResponseError(reply string, log *logrus.Entry) func(context.Context, *qms.SubscriptionAddonListResponse, error) {
	return func(ctx context.Context, response *qms.SubscriptionAddonListResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}
}

func (a *App) sendSubscriptionAddonResponseError(reply string, log *logrus.Entry) func(context.Context, *qms.SubscriptionAddonResponse, error) {
	return func(ctx context.Context, response *qms.SubscriptionAddonResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}
}

func (a *App) AddAddonHandler(subject, reply string, request *qms.AddAddonRequest) {
	var err error

	ctx, span := qmsinit.InitAddAddonRequest(request, subject)
	defer span.End()

	log := log.WithField("context", "adding new available addon")
	response := qmsinit.NewAddonResponse()
	sendError := a.sendAddonResponseError(reply, log)
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

	tx, err := d.Begin()
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	defer tx.Rollback()

	if reqAddon.ResourceType.Name != "" && reqAddon.ResourceType.Uuid == "" {
		lookupRT, err = d.GetResourceTypeByName(ctx, reqAddon.ResourceType.Name, db.WithTX(tx))
		if err != nil {
			sendError(ctx, response, err)
			return
		}
	} else {
		lookupRT, err = d.GetResourceType(ctx, reqAddon.ResourceType.Uuid, db.WithTX(tx))
		if err != nil {
			sendError(ctx, response, err)
			return
		}
	}

	newAddon := db.NewAddonFromQMS(request.Addon)
	newAddon.ResourceType = *lookupRT

	newID, err := d.AddAddon(ctx, newAddon, db.WithTX(tx))
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	if err = tx.Commit(); err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Addon = newAddon.ToQMSType()
	response.Addon.Uuid = newID

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

// ListAddonsHandler lists all of the available add-ons in the system. These are
// the ones that can be applied to a subscription, not the ones that have been
// applied already.
func (a *App) ListAddonsHandler(subject, reply string, request *qms.NoParamsRequest) {
	var err error

	ctx, span := qmsinit.InitNoParamsRequest(request, subject)
	defer span.End()

	log := log.WithField("context", "list addons")
	sendError := a.sendAddonListResponseError(reply, log)
	response := qmsinit.NewAddonListResponse()
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

func (a *App) UpdateAddonHandler(subject, reply string, request *qms.UpdateAddonRequest) {
	var err error

	log := log.WithField("context", "update addon")

	response := qmsinit.NewAddonResponse()

	sendError := func(ctx context.Context, response *qms.AddonResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	ctx, span := qmsinit.InitUpdateAddonRequest(request, subject)
	defer span.End()

	d := db.New(a.db)

	if request.Addon.Uuid == "" {
		sendError(ctx, response, errors.New("uuid must be set in the request"))
		return
	}

	updateAddon := db.NewUpdateAddonFromQMS(request)

	result, err := d.UpdateAddon(ctx, updateAddon)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Addon = result.ToQMSType()

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) DeleteAddonHandler(subject, reply string, request *requests.ByUUID) {
	var err error

	log := log.WithField("context", "delete addon")

	response := qmsinit.NewAddonResponse()

	sendError := func(ctx context.Context, response *qms.AddonResponse, err error) {
		log.Error(err)
		response.Error = serrors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	ctx, span := reqinit.InitByUUID(request, subject)
	defer span.End()

	d := db.New(a.db)

	if err = d.DeleteAddon(ctx, request.Uuid); err != nil {
		sendError(ctx, response, err)
		return
	}

	response.Addon = &qms.Addon{
		Uuid: request.Uuid,
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

// ListSubscriptionAddonsHandler lists the add-ons that have been applied to the
// indicated subscription.
func (a *App) ListSubscriptionAddonsHandler(subject, reply string, request *requests.ByUUID) {
	var err error

	ctx, span := reqinit.InitByUUID(request, subject)
	defer span.End()

	log := log.WithField("context", "listing subscription add-ons")
	response := qmsinit.NewSubscriptionAddonListResponse()
	sendError := a.sendSubscriptionAddonListResponseError(reply, log)
	d := db.New(a.db)

	results, err := d.ListSubscriptionAddons(ctx, request.Uuid)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	for _, addon := range results {
		response.SubscriptionAddons = append(response.SubscriptionAddons, addon.ToQMSType())
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) AddSubscriptionAddonHandler(subject, reply string, request *requests.AssociateByUUIDs) {
	var err error

	ctx, span := reqinit.InitAssociateByUUIDs(request, subject)
	defer span.End()

	log := log.WithField("context", "adding subscription add-on")
	response := qmsinit.NewSubscriptionAddonResponse()
	sendError := a.sendSubscriptionAddonResponseError(reply, log)
	d := db.New(a.db)

	subscriptionID := request.ParentUuid
	if subscriptionID == "" {
		sendError(ctx, response, errors.New("parent_uuid must be set to the subscription UUID"))
		return
	}

	addonID := request.ChildUuid
	if addonID == "" {
		sendError(ctx, response, errors.New("child_id must be set to the add-on UUID"))
		return
	}

	tx, err := d.Begin()
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	defer tx.Rollback()

	subAddon, err := d.AddSubscriptionAddon(ctx, subscriptionID, addonID, db.WithTXRollbackCommit(tx, false, false))
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	quotaValue, quotaFound, err := d.GetCurrentQuota(
		ctx,
		subAddon.Addon.ResourceType.ID,
		subscriptionID,
		db.WithTXRollbackCommit(tx, false, false),
	)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	quotaValue = quotaValue + subAddon.Amount
	if err = d.UpsertQuota(
		ctx,
		quotaFound,
		quotaValue,
		subAddon.Addon.ResourceType.ID,
		subscriptionID,
		db.WithTXRollbackCommit(tx, false, false),
	); err != nil {
		sendError(ctx, response, err)
		return
	}

	if err = tx.Commit(); err != nil {
		sendError(ctx, response, err)
		return
	}

	response.SubscriptionAddon = subAddon.ToQMSType()

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) DeleteSubscriptionAddonHandler(subject, reply string, request *requests.ByUUID) {
	var err error

	ctx, span := reqinit.InitByUUID(request, subject)
	defer span.End()

	log := log.WithField("context", "deleting subscription add-ons")
	response := qmsinit.NewSubscriptionAddonResponse()
	sendError := a.sendSubscriptionAddonResponseError(reply, log)
	d := db.New(a.db)

	// Get the subscription add-on ID out of the request.
	subAddonID := request.Uuid
	if subAddonID == "" {
		sendError(ctx, response, errors.New("subscription add-on UUID must be set"))
		return
	}

	/// Start the database transaction.
	tx, err := d.Begin()
	if err != nil {
		sendError(ctx, response, err)
		return
	}
	defer tx.Rollback()

	// Get the subscription add-on details from the database. Needed to modify
	// the quota value.
	subAddon, err := d.GetSubscriptionAddonByID(ctx, subAddonID, db.WithTX(tx))
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	// Get the current quota value.
	quotaValue, quotaFound, err := d.GetCurrentQuota(
		ctx,
		subAddon.Addon.ResourceType.ID,
		subAddon.Subscription.ID,
		db.WithTXRollbackCommit(tx, false, false),
	)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	// Update the quota value by subtracting the amount configured in the
	// subscription add-on. We don't want the available add-on value, we want
	// the subscription add-on value, which may have been modified from the
	// available add-on value.
	quotaValue = quotaValue - subAddon.Amount
	if err = d.UpsertQuota(
		ctx,
		quotaFound,
		quotaValue,
		subAddon.Addon.ResourceType.ID,
		subAddon.Subscription.ID,
		db.WithTXRollbackCommit(tx, false, false),
	); err != nil {
		sendError(ctx, response, err)
		return
	}

	// Delete the subscription add-on.
	if err = d.DeleteSubscriptionAddon(ctx, subAddonID, db.WithTX(tx)); err != nil {
		sendError(ctx, response, err)
		return
	}

	// Commit all of the changes.
	if err = tx.Commit(); err != nil {
		sendError(ctx, response, err)
		return
	}

	// Return the response.
	response.SubscriptionAddon = subAddon.ToQMSType()

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) UpdateSubscriptionAddonHandler(subject, reply string, request *qms.UpdateSubscriptionAddonRequest) {
	var err error

	ctx, span := qmsinit.InitUpdateSubscriptionAddonRequest(request, subject)
	defer span.End()

	log := log.WithField("context", "update subscription addon")
	response := qmsinit.NewSubscriptionAddonResponse()
	sendError := a.sendSubscriptionAddonResponseError(reply, log)
	d := db.New(a.db)

	if request.SubscriptionAddon.Uuid == "" {
		sendError(ctx, response, errors.New("uuid must be set in the request"))
		return
	}

	updateSubAddon := db.NewUpdateSubscriptionAddonFromQMS(request)

	result, err := d.UpdateSubscriptionAddon(ctx, updateSubAddon)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	response.SubscriptionAddon = result.ToQMSType()

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
