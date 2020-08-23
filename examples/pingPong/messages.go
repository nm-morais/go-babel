package main

import (
	"github.com/nm-morais/go-babel/pkg/message"
)

const PingMessageType message.ID = 1001
const PongMessageType message.ID = 1002

type Ping struct {
	Payload string
}

type PingSerializer struct {
}

var pingSerializer = PingSerializer{}

func (Ping) Type() message.ID {
	return PingMessageType
}

func (msg Ping) Serializer() message.Serializer {
	return pingSerializer
}

func (msg Ping) Deserializer() message.Deserializer {
	return pingSerializer
}

func (PingSerializer) Serialize(message message.Message) []byte {
	return []byte(message.(Ping).Payload)

}

func (PingSerializer) Deserialize(toDeserialize []byte) message.Message {
	return Ping{string(toDeserialize)}
}

type Pong struct {
	Payload string
}

type PongSerializer struct {
}

var pongSerializer = PongSerializer{}

func (Pong) Type() message.ID {
	return PongMessageType
}

func (msg Pong) Serializer() message.Serializer {
	return pongSerializer
}

func (msg Pong) Deserializer() message.Deserializer {
	return pongSerializer
}

func (PongSerializer) Serialize(message message.Message) []byte {
	return []byte(message.(Pong).Payload)
}

func (PongSerializer) Deserialize(toDeserialize []byte) message.Message {
	return Pong{Payload: string(toDeserialize)}
}
