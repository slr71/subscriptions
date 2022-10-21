package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/subscriptions/db"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type App struct {
	natsConn      *nats.EncodedConn
	db            *sqlx.DB
	baseSubject   string
	baseQueue     string
	subscriptions []*nats.Subscription
	userSuffix    string
}

func New(natsConn *nats.EncodedConn, db *sqlx.DB, baseQueue, baseSubject, userSuffix string) *App {
	return &App{
		natsConn:    natsConn,
		db:          db,
		baseSubject: baseSubject,
		baseQueue:   baseQueue,
		userSuffix:  userSuffix,
	}
}

func (a *App) natsSubject(fields ...string) string {
	trimmed := strings.TrimSuffix(
		strings.TrimSuffix(a.baseSubject, ".*"),
		".>",
	)
	addFields := strings.Join(fields, ".")
	return fmt.Sprintf("%s.%s", trimmed, addFields)
}

func (a *App) natsQueue(fields ...string) string {
	return fmt.Sprintf("%s.%s", a.baseQueue, strings.Join(fields, "."))
}

func (a *App) queueSub(name string, handler nats.Handler) {
	var err error

	subject := a.natsSubject(name)
	queue := a.natsQueue(name)

	s, err := a.natsConn.QueueSubscribe(subject, queue, handler)
	if err != nil {
		log.Fatal(err)
	}

	a.subscriptions = append(a.subscriptions, s)
}

func (a *App) Init() *App {
	a.queueSub("user.updates.get", a.GetUserUpdatesHandler)
	a.queueSub("user.updates.add", a.AddUserUpdateHandler)
	return a
}

func (a *App) validateUpdate(request *qms.AddUpdateRequest) (string, error) {
	username := strings.TrimSuffix(request.Update.User.Username, a.userSuffix)
	if username == "" {
		return "", ErrInvalidUsername
	}

	if request.Update.ResourceType.Name == "" || !lo.Contains(
		db.ResourceTypeNames,
		request.Update.ResourceType.Name,
	) {
		return username, ErrInvalidResourceName
	}

	if request.Update.ResourceType.Unit == "" || !lo.Contains(
		db.ResourceTypeUnits,
		request.Update.ResourceType.Unit,
	) {
		return username, ErrInvalidResourceUnit
	}

	if request.Update.Operation.Name == "" || !lo.Contains(
		db.UpdateOperationNames,
		request.Update.Operation.Name,
	) {
		return username, ErrInvalidOperationName
	}

	if request.Update.ValueType == "" || !lo.Contains(
		[]string{db.UsagesTrackedMetric, db.QuotasTrackedMetric},
		request.Update.ValueType,
	) {
		return username, ErrInvalidValueType
	}

	if request.Update.EffectiveDate == nil {
		return username, ErrInvalidEffectiveDate
	}

	return username, nil
}

func (a *App) GetUserUpdatesHandler(subject, reply string, request *qms.UpdateListRequest) {
	var err error

	log := log.WithFields(logrus.Fields{"context": "get all user updates over nats"})
	response := pbinit.NewQMSUpdateListResponse()
	ctx, span := pbinit.InitQMSUpdateListRequest(request, subject)
	defer span.End()

	username := request.User.Username
	if username == "" {
		response.Error = gotelnats.InitServiceError(
			ctx, err, &gotelnats.ErrorOptions{
				ErrorCode: svcerror.ErrorCode_BAD_REQUEST,
			},
		)
	}

	log = log.WithFields(logrus.Fields{"user": username})

	d := db.New(a.db)

	mUpdates, err := d.UserUpdates(ctx, username)
	if err != nil {
		log.Error(err)
		response.Error = gotelnats.InitServiceError(
			ctx, err, &gotelnats.ErrorOptions{
				ErrorCode: natsStatusCode(err),
			},
		)
	}

	for _, mu := range mUpdates {
		response.Updates = append(response.Updates, &qms.Update{
			Uuid:          mu.ID,
			EffectiveDate: timestamppb.New(mu.EffectiveDate),
			ValueType:     mu.ValueType,
			Value:         mu.Value,
			ResourceType: &qms.ResourceType{
				Uuid: mu.ResourceTypeID,
				Name: mu.ResourceTypeName,
				Unit: mu.ResourceTypeUnit,
			},
			Operation: &qms.UpdateOperation{
				Uuid: mu.UpdateOperationID,
				Name: mu.UpdateOperationName,
			},
			User: &qms.QMSUser{
				Uuid:     mu.UserID,
				Username: mu.Username,
			},
		})
	}

	if err = gotelnats.PublishResponse(ctx, a.natsConn, reply, response); err != nil {
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
		response.Error = natsError(ctx, err)
		if err = gotelnats.PublishResponse(ctx, a.natsConn, reply, response); err != nil {
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
		userID, err = d.GetUserID(ctx, username)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
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
		mUpdate := &db.Update{
			ValueType:           request.Update.ValueType,
			Value:               request.Update.Value,
			EffectiveDate:       request.Update.EffectiveDate.AsTime(),
			ResourceTypeID:      resourceTypeID,
			ResourceTypeName:    request.Update.ResourceType.Name,
			ResourceTypeUnit:    request.Update.ResourceType.Unit,
			UserID:              userID,
			Username:            request.Update.User.Username,
			UpdateOperationID:   operationID,
			UpdateOperationName: request.Update.Operation.Name,
		}

		log.Info("adding update to the database")
		// Add the update to the database.
		update, err = d.AddUserUpdate(ctx, mUpdate)
		if err != nil {
			sendError(ctx, response, err)
			return
		}
		log.Info("done adding update to the database")

		log.Infof("value type %s", update.ValueType)

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
		rUpdate := qms.Update{
			Uuid:      update.ID,
			ValueType: update.ValueType,
			Value:     update.Value,
			ResourceType: &qms.ResourceType{
				Uuid: update.ResourceTypeID,
				Name: update.ResourceTypeName,
				Unit: update.ResourceTypeUnit,
			},
			EffectiveDate: timestamppb.New(update.EffectiveDate),
			Operation: &qms.UpdateOperation{
				Uuid: update.UpdateOperationID,
			},
			User: &qms.QMSUser{
				Uuid:     update.UserID,
				Username: update.Username,
			},
		}

		response.Update = &rUpdate
	}

	// Send the response to the caller
	if err = gotelnats.PublishResponse(ctx, a.natsConn, reply, response); err != nil {
		log.Error(err)
	}

}
