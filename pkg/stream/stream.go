package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
)

type Stream interface {
	ListenAddr() net.Addr
	Dial(addr net.Addr) errors.Error
	DialWithTimeout(addr net.Addr, timeout time.Duration) errors.Error
	Read(buf []byte) (int, error)
	Write(msgBytes []byte) (int, error)
	SetReadTimeout(deadline time.Time)
	Listen() (Listener, errors.Error)
	Close() error
}

type Listener interface {
	Accept() (Stream, errors.Error)
}
