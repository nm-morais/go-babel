package message

import "github.com/nm-morais/DeMMon/go-babel/pkg/serialization"

const MaxMessageBytes = 2048

type ID uint16

type Message interface {
	Type() ID
	Serializer() serialization.Serializer
	Deserializer() serialization.Deserializer
}
