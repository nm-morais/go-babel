package notificationHub

import (
	internalProto "github.com/nm-morais/DeMMon/go-babel/internal/protocol"
	"github.com/nm-morais/DeMMon/go-babel/pkg/notification"
	"github.com/nm-morais/DeMMon/go-babel/pkg/protocol"
	"sync"
)

type NotificationHub interface {
	AddListener(id notification.ID, listener *internalProto.WrapperProtocol)
	RemoveListener(listenerID notification.ID, protoID protocol.ID)
	AddNotification(notification notification.Notification)
}

type notificationHub struct {
	listenersMutex *sync.RWMutex
	listeners      map[notification.ID][]*internalProto.WrapperProtocol
}

func NewNotificationHub() NotificationHub {
	return &notificationHub{
		listenersMutex: &sync.RWMutex{},
		listeners:      map[notification.ID][]*internalProto.WrapperProtocol{},
	}
}

func (hub *notificationHub) RemoveListener(id notification.ID, protoID protocol.ID) {
	hub.listenersMutex.Lock()
	currListeners, ok := hub.listeners[id]
	if !ok {
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
	hub.listenersMutex.Unlock()
}

func (hub *notificationHub) AddListener(id notification.ID, wrapperProtocol *internalProto.WrapperProtocol) {
	hub.listenersMutex.Lock()
	currListeners, ok := hub.listeners[id]
	if !ok {
		hub.listeners[id] = []*internalProto.WrapperProtocol{wrapperProtocol}
		return
	}
	currListeners = append(currListeners, wrapperProtocol)
	hub.listeners[id] = currListeners
	hub.listenersMutex.Unlock()
}

func (hub *notificationHub) AddNotification(n notification.Notification) {
	hub.listenersMutex.RLock()
	currListeners, ok := hub.listeners[n.ID()]
	if !ok {
		return
	}
	for _, listener := range currListeners {
		listener.DeliverNotification(n)
	}
	hub.listenersMutex.RUnlock()
}
