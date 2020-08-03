package transport

import (
	"github.com/nm-morais/DeMMon/go-babel/pkg"
	"github.com/nm-morais/DeMMon/go-babel/pkg/peer"
)

type Transport interface {
	Listen() <-chan Transport
	Peer() peer.Peer
	Dial(peer peer.Peer) <-chan pkg.Error
	PipeToMessageChan() <-chan []byte
	MessageChan() <-chan []byte
	SendMessage(message []byte) pkg.Error
	Close()
}
