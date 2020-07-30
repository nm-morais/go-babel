package request

import (
	. "github.com/DeMMon/go-babel/pkg"
)

type Request interface {
	ID() ID
	Params() []string
}
