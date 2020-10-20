package protocolManager

import (
	"net"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	. "github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/serializationManager"
	"github.com/nm-morais/go-babel/pkg/timer"
)

type ProtocolManager interface {
	SendMessage(toSend message.Message, destPeer Peer, origin protocol.ID, destination protocol.ID)
	SendMessageAndDisconnect(toSend message.Message, destPeer Peer, origin protocol.ID, destination protocol.ID)
	SendMessageSideStream(toSend message.Message, peer peer.Peer, addr net.Addr, sourceProtoID protocol.ID, destProto protocol.ID)
	SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) errors.Error
	SendNotification(notification notification.Notification) errors.Error

	RegisterListenAddr(addr net.Addr)
	Dial(dialingProto protocol.ID, peer peer.Peer, toDial net.Addr) errors.Error
	InConnRequested(dialerProto protocol.ID, dialer Peer) bool
	DialError(sourceProto protocol.ID, dialedPeer Peer)

	DialSuccess(dialerProto protocol.ID, dialedPeer Peer) bool
	OutTransportFailure(dialerProto protocol.ID, peer Peer)
	Disconnect(source protocol.ID, toDc Peer)

	DeliverMessage(sender peer.Peer, message message.Message, destProto protocol.ID)
	DeliverTimer(timer timer.Timer, destProto protocol.ID)

	RegisterProtocol(protocol protocol.Protocol) errors.Error
	RegisteredProtos() []protocol.ID

	SerializationManager() serializationManager.SerializationManager

	RegisterNotificationHandler(protoID protocol.ID, notification notification.Notification, handler handlers.NotificationHandler) errors.Error
	RegisterTimerHandler(protoID protocol.ID, timer timer.ID, handler handlers.TimerHandler) errors.Error
	RegisterRequestHandler(protoID protocol.ID, request request.ID, handler handlers.RequestHandler) errors.Error
	RegisterRequestReplyHandler(protoID protocol.ID, replyId request.ID, handler handlers.ReplyHandler) errors.Error
	RegisterMessageHandler(protoID protocol.ID, message message.Message, handler handlers.MessageHandler) errors.Error

	CancelTimer(timerID int) errors.Error
	RegisterTimer(origin protocol.ID, timer timer.Timer) int

	SelfPeer() Peer

	Start()
}
