package serializationManager

import "github.com/nm-morais/go-babel/pkg/message"

type SerializationManager interface {
	RegisterDeserializer(id message.ID, serializer message.Deserializer)
	RegisterSerializer(id message.ID, serializer message.Serializer)
	Deserialize(id message.ID, bytes []byte) (message.Message, error)
	Serialize(message message.Message) ([]byte, error)
}
