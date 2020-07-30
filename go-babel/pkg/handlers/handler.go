package handlers

import . "github.com/DeMMon/go-babel/pkg"

type Handler interface {
	ID() ID
	Handle(interface{})
}
