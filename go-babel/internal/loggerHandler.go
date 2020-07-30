package internal

import (
	. "github.com/DeMMon/go-babel/pkg"
	. "github.com/DeMMon/go-babel/pkg/handlers"
	"github.com/sirupsen/logrus"
)

type LoggerHandler struct {
	logger  logrus.Logger
	Handler Handler
	id      ID
}

var LoggerHandlerID = "LoggerHandler"

func NewHandler(handler Handler) *LoggerHandler {
	return &LoggerHandler{
		logger:  logrus.Logger{},
		Handler: handler,
		id:      LoggerHandlerID,
	}
}

func (lg *LoggerHandler) ID() ID {
	return lg.id
}

func (lg *LoggerHandler) Handle(toHandle interface{}) {
	lg.logger.Info("handler %s triggered by event %+v", lg.Handler.ID, toHandle)
	lg.Handler.Handle(toHandle)
}
