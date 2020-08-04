package transport

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
)

type Transport interface {
	Listen() <-chan Transport
	Dial(peer peer.Peer) <-chan errors.Error
	PipeBytesToChan() <-chan []byte
	MessageChan() <-chan []byte
	SendMessage(message []byte) errors.Error
	Close()
}
