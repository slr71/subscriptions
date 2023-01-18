package app

import (
	"github.com/cyverse-de/p/go/qms"
)

func (a *App) GetSubscriptionHandler(subject, reply string, request *qms.RequestByUsername) {
	a.GetUserSummaryHandler(subject, reply, request)
}
