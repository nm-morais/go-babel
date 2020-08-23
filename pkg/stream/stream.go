package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
)

type Stream interface {
	ListenAddr() net.Addr
	Dial(addr peer.Peer) errors.Error
	Read(buf []byte) (int, error)
	Write(msgBytes []byte) (int, error)
	SetReadTimeout(duration time.Duration)
	Listen() (Listener, errors.Error)
	Close() error
}

type Listener interface {
	Accept() (Stream, errors.Error)
}
