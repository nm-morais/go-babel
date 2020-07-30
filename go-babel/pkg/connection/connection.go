package connection

import (
	. "github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"net"
)

type Connection interface {
	SendMessage(peer peer.Peer)
}

type BidirectionalConnection interface {
	Protocol() protocol.Protocol
	SendMessage(message message.Message) Error
	HandleConn(peer peer.Peer, conn net.Conn)
	ConnectTo(peer peer.Peer)
}
