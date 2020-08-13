package stream

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
	"net"
	"time"
)

type Stream interface {
	ListenAddr() net.Addr
	Dial(addr peer.Peer) errors.Error
	Read(buf []byte) (int, error)
	Write(msgBytes []byte) (int, error)
	SetReadTimeout(duration time.Duration)
	Close() errors.Error
	Listen() (Listener, errors.Error)
}

type Listener interface {
	Accept() (Stream, errors.Error)
}
