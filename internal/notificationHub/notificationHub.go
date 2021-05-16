package notificationHub

import (
	"errors"
	"reflect"
	"sync"

	internalProto "github.com/nm-morais/go-babel/internal/protocol"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

type NotificationHub interface {
	AddListener(id notification.ID, listener *internalProto.WrapperProtocol)
	RemoveListener(listenerID notification.ID, protoID protocol.ID)
	AddNotification(notification notification.Notification)
	Logger() *logrus.Logger
}

var (
	ErrProtoNotFound = errors.New("Proto not found")
)

type notificationHub struct {
	logger         *logrus.Logger
	listenersMutex *sync.RWMutex
	listeners      map[notification.ID][]*internalProto.WrapperProtocol
}

func NewNotificationHub() NotificationHub {
	return &notificationHub{
		logger:         logs.NewLogger("NotificationHub"),
		listenersMutex: &sync.RWMutex{},
		listeners:      map[notification.ID][]*internalProto.WrapperProtocol{},
	}
}
func (hub *notificationHub) Logger() *logrus.Logger {
	return hub.logger
}
func (hub *notificationHub) RemoveListener(id notification.ID, protoID protocol.ID) {
	hub.listenersMutex.Lock()
	defer hub.listenersMutex.Unlock()
	currListeners, ok := hub.listeners[id]
	if !ok {
		hub.logger.Panic(ErrProtoNotFound.Error())
		return
	}

	for idx, listener := range currListeners {
		if listener.ID() == protoID {
			currListeners[idx] = currListeners[len(currListeners)-1]
			hub.listeners[id] = currListeners[:len(currListeners)-1]
		}
	}

	if len(hub.listeners[id]) == 0 {
		delete(hub.listeners, id)
	}
}

func (hub *notificationHub) AddListener(id notification.ID, wrapperProtocol *internalProto.WrapperProtocol) {
	hub.listenersMutex.Lock()
	defer hub.listenersMutex.Unlock()
	currListeners, ok := hub.listeners[id]
	if !ok {
		hub.listeners[id] = []*internalProto.WrapperProtocol{wrapperProtocol}
		return
	}

	currListeners = append(currListeners, wrapperProtocol)
	hub.listeners[id] = currListeners
}

func (hub *notificationHub) AddNotification(n notification.Notification) {
	hub.listenersMutex.RLock()
	defer hub.listenersMutex.RUnlock()
	currListeners, ok := hub.listeners[n.ID()]
	if !ok {
		hub.logger.Warnf("No listener for notification of type: %s", reflect.TypeOf(n))
		return
	}

	for _, listener := range currListeners {
		hub.logger.Info("Delivering notification")
		listener.DeliverNotification(n)
	}

}
