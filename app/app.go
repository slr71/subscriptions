package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/cyverse-de/subscriptions/errors"
	"github.com/cyverse-de/subscriptions/natscl"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "apps"})

type App struct {
	client         *natscl.Client
	db             *sqlx.DB
	userSuffix     string
	ReportOverages bool
}

func New(client *natscl.Client, db *sqlx.DB, userSuffix string) *App {
	return &App{
		client:         client,
		db:             db,
		userSuffix:     userSuffix,
		ReportOverages: true,
	}
}

func (a *App) FixUsername(username string) (string, error) {
	u := strings.TrimSuffix(username, a.userSuffix)
	if u == "" {
		return "", errors.ErrInvalidUsername
	}
	return u, nil
}

func (a *App) validateUpdate(request *qms.AddUpdateRequest) (string, error) {
	username, err := a.FixUsername(request.Update.User.Username)
	if err != nil {
		return "", err
	}

	if request.Update.ResourceType.Name == "" || !lo.Contains(
		[]string(db.ResourceTypeNames),
		string(request.Update.ResourceType.Name),
	) {
		return username, errors.ErrInvalidResourceName
	}

	if request.Update.ResourceType.Unit == "" || !lo.Contains(
		[]string(db.ResourceTypeUnits),
		string(request.Update.ResourceType.Unit),
	) {
		return username, errors.ErrInvalidResourceUnit
	}

	if request.Update.Operation.Name == "" || !lo.Contains(
		[]string(db.UpdateOperationNames),
		string(request.Update.Operation.Name),
	) {
		return username, errors.ErrInvalidOperationName
	}

	if request.Update.ValueType == "" || !lo.Contains(
		[]string{db.UsagesTrackedMetric, db.QuotasTrackedMetric},
		string(request.Update.ValueType),
	) {
		return username, errors.ErrInvalidValueType
	}

	if request.Update.EffectiveDate == nil {
		return username, errors.ErrInvalidEffectiveDate
	}

	return username, nil
}

func (a *App) GetUserUpdatesHandler(subject, reply string, request *qms.UpdateListRequest) {
	var err error

	log := log.WithFields(logrus.Fields{"context": "get all user updates over nats"})
	response := pbinit.NewQMSUpdateListResponse()
	ctx, span := pbinit.InitQMSUpdateListRequest(request, subject)
	defer span.End()

	// Avoid duplicating a lot of error reporting code.
	sendError := func(ctx context.Context, response *qms.UpdateListResponse, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	username, err := a.FixUsername(request.User.Username)
	if err != nil {
		sendError(ctx, response, err)
		return

	}

	log = log.WithFields(logrus.Fields{"user": username})

	d := db.New(a.db)

	mUpdates, err := d.UserUpdates(ctx, username)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	for _, mu := range mUpdates {
		response.Updates = append(response.Updates, &qms.Update{
			Uuid:          mu.ID,
			EffectiveDate: timestamppb.New(mu.EffectiveDate),
			ValueType:     mu.ValueType,
			Value:         mu.Value,
			ResourceType: &qms.ResourceType{
				Uuid: mu.ResourceType.ID,
				Name: mu.ResourceType.Name,
				Unit: mu.ResourceType.Unit,
			},
			Operation: &qms.UpdateOperation{
				Uuid: mu.UpdateOperation.ID,
				Name: mu.UpdateOperation.Name,
			},
			User: &qms.QMSUser{
				Uuid:     mu.User.ID,
				Username: mu.User.Username,
			},
		})
	}

	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}

func (a *App) AddUserUpdateHandler(subject, reply string, request *qms.AddUpdateRequest) {
	var (
		err                                 error
		userID, resourceTypeID, operationID string
		update                              *db.Update
	)

	// Initialize the response.
	log := log.WithFields(logrus.Fields{"context": "add a user update over nats"})
	response := pbinit.NewQMSAddUpdateResponse()
	ctx, span := pbinit.InitQMSAddUpdateRequest(request, subject)
	defer span.End()

	// Avoid duplicating a lot of error reporting code.
	sendError := func(ctx context.Context, response *qms.AddUpdateResponse, err error) {
		log.Error(err)
		response.Error = errors.NatsError(ctx, err)
		if err = a.client.Respond(ctx, reply, response); err != nil {
			log.Error(err)
		}
	}

	d := db.New(a.db)

	username, err := a.validateUpdate(request)
	if err != nil {
		sendError(ctx, response, err)
		return
	}

	log = log.WithFields(logrus.Fields{"user": username})

	// Get the userID if it's not provided
	if request.Update.User.Uuid == "" {
		log.Infof("getting user ID for %s", username)
		user, err := d.EnsureUser(ctx, username)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		userID = user.ID
		log.Infof("user ID for %s is %s", username, userID)
	} else {
		userID = request.Update.User.Uuid
		log.Infof("user ID from request is %s", userID)
	}

	// Get the resource type id if it's not provided.
	if request.Update.ResourceType.Uuid == "" {
		log.Infof("getting resource type id for resource '%s'", request.Update.ResourceType.Name)
		resourceTypeID, err = d.GetResourceTypeID(
			ctx,
			request.Update.ResourceType.Name,
			request.Update.ResourceType.Unit,
		)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		log.Infof("resource type id for resource %s is '%s'", request.Update.ResourceType.Name, resourceTypeID)
	} else {
		resourceTypeID = request.Update.ResourceType.Uuid
		log.Infof("resource type id from request is %s", resourceTypeID)
	}

	// Get the operation id if it's not provided.
	if request.Update.Operation.Uuid == "" {
		log.Infof("getting operation ID for %s", request.Update.Operation.Name)
		operationID, err = d.GetOperationID(
			ctx,
			request.Update.Operation.Name,
		)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		log.Infof("operation ID for %s is %s", request.Update.Operation.Name, operationID)
	} else {
		operationID = request.Update.Operation.Uuid
		log.Infof("using operation ID %s from request", request.Update.Operation.Uuid)
	}

	// construct the model.Update
	if response.Error == nil {
		update = &db.Update{
			ValueType:     request.Update.ValueType,
			Value:         request.Update.Value,
			EffectiveDate: request.Update.EffectiveDate.AsTime(),
			ResourceType: db.ResourceType{
				ID:   resourceTypeID,
				Name: request.Update.ResourceType.Name,
				Unit: request.Update.ResourceType.Unit,
			},
			User: db.User{
				ID:       userID,
				Username: username,
			},
			UpdateOperation: db.UpdateOperation{
				ID:   operationID,
				Name: request.Update.Operation.Name,
			},
		}

		log.Info("adding update to the database")
		_, err = d.AddUserUpdate(ctx, update)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		log.Info("done adding update to the database")

		switch update.ValueType {
		case db.UsagesTrackedMetric:
			log.Info("processing update for usage")
			if err = d.ProcessUpdateForUsage(ctx, update); err != nil {
				sendError(ctx, response, err)
				return
			}
			log.Info("after processing update for usage")

		case db.QuotasTrackedMetric:
			log.Info("processing update for quota")
			if err = d.ProcessUpdateForQuota(ctx, update); err != nil {
				sendError(ctx, response, err)
				return
			}
			log.Info("after processing update for quota")

		default:
			err = fmt.Errorf("unknown value type in update: %s", update.ValueType)
			sendError(ctx, response, err)
			return
		}

		// Set up the object for the response.
		response.Update = &qms.Update{
			Uuid:      update.ID,
			ValueType: update.ValueType,
			Value:     update.Value,
			ResourceType: &qms.ResourceType{
				Uuid: update.ResourceType.ID,
				Name: update.ResourceType.Name,
				Unit: update.ResourceType.Unit,
			},
			EffectiveDate: timestamppb.New(update.EffectiveDate),
			Operation: &qms.UpdateOperation{
				Uuid: update.UpdateOperation.ID,
				Name: update.UpdateOperation.Name,
			},
			User: &qms.QMSUser{
				Uuid:     update.User.ID,
				Username: update.User.Username,
			},
		}
	}

	// Send the response to the caller
	if err = a.client.Respond(ctx, reply, response); err != nil {
		log.Error(err)
	}
}
