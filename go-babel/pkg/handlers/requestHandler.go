package handlers

import (
	. "github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/request"
)

type RequestHandler interface {
	ID() ID
	HandledObjectID() ID
	Handle(request request.Request) Error
}
