package serializationManager

import "github.com/nm-morais/go-babel/pkg/message"

type SerializationManager interface {
	GetDeserializer(id message.ID) message.Deserializer
	GetSerializer(id message.ID) message.Serializer
	RegisterDeserializer(id message.ID, serializer message.Deserializer)
	RegisterSerializer(id message.ID, serializer message.Serializer)
	Deserialize(id message.ID, bytes []byte) message.Message
	Serialize(message message.Message) []byte
}
