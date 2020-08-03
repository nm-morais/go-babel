package pingPong

import (
	"bytes"
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/message"
	"io"
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
	protoMsg := message.(Ping)
	buf := &bytes.Buffer{}
	writer := io.Writer(buf)
	if err := binary.Write(writer, binary.BigEndian, protoMsg); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (PingSerializer) Deserialize(toDeserialize []byte) message.Message {
	buf := bytes.NewBuffer(toDeserialize)
	newMsg := &Ping{}
	reader := io.Reader(buf)
	if err := binary.Read(reader, binary.BigEndian, newMsg); err != nil {
		panic(err)
	}
	return newMsg
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
	protoMsg := message.(Pong)
	buf := &bytes.Buffer{}
	writer := io.Writer(buf)
	if err := binary.Write(writer, binary.BigEndian, protoMsg); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (PongSerializer) Deserialize(toDeserialize []byte) message.Message {
	buf := bytes.NewBuffer(toDeserialize)
	newMsg := &Pong{}
	reader := io.Reader(buf)
	if err := binary.Read(reader, binary.BigEndian, newMsg); err != nil {
		panic(err)
	}
	return newMsg
}
