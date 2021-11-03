package streamManager

import (
	"net"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

type StreamManager interface {
	AcceptConnectionsAndNotify(listenAddr net.Addr) (done chan interface{})
	DialAndNotify(dialingProto protocol.ID, peer peer.Peer, toDial net.Addr) errors.Error
	SendMessageSideStream(
		toSend message.Message,
		peer peer.Peer,
		addr net.Addr,
		sourceProtoID protocol.ID,
		destProtos protocol.ID,
	) errors.Error
	SendMessage(toSend message.Message, peer peer.Peer, origin protocol.ID, destination protocol.ID, batch bool)
	Disconnect(disconnectingProto protocol.ID, p peer.Peer)
	DisconnectInStream(disconnectingProto protocol.ID, p peer.Peer)
	Logger() *logrus.Logger
}
