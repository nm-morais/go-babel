package message

type ID uint16

type Deserializer interface {
	Deserialize(bytes []byte) Message
}

type Serializer interface {
	Serialize(msg Message) []byte
}

type Message interface {
	Type() ID
	Serializer() Serializer
	Deserializer() Deserializer
}
