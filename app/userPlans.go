package app

import (
	"github.com/cyverse-de/p/go/qms"
)

func (a *App) GetUserPlanHandler(subject, reply string, request *qms.RequestByUsername) {
	a.GetUserSummary(subject, reply, request)
}
