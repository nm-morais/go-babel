package serialization

import "github.com/nm-morais/DeMMon/go-babel/pkg/message"

type Deserializer interface {
	Deserialize(bytes []byte) message.Message
}

type Serializer interface {
	Serialize(msg message.Message) []byte
}
