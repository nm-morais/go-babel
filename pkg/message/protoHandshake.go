package message

import (
	"bytes"
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"io"
)

var protoHandshakeMessageSerializer = AppMessageWrapperSerializer{}

type ProtoHandshakeMessage struct {
	Protos []protocol.ID
}

func NewProtoHandshakeMessage(protos []protocol.ID) Message {
	return &ProtoHandshakeMessage{
		Protos: protos,
	}
}

func (msg ProtoHandshakeMessage) Type() ID {
	panic("implement me")
}

type ProtoHandshakeMessageSerializer struct{}

func (msg ProtoHandshakeMessage) Serializer() Serializer {
	return protoHandshakeMessageSerializer
}

func (msg ProtoHandshakeMessage) Deserializer() Deserializer {
	return protoHandshakeMessageSerializer
}

func (msg ProtoHandshakeMessageSerializer) Serialize(message Message) []byte {
	protoMsg := message.(ProtoHandshakeMessage)
	buf := &bytes.Buffer{}
	writer := io.Writer(buf)
	if err := binary.Write(writer, binary.BigEndian, protoMsg); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (msg ProtoHandshakeMessageSerializer) Deserialize(toDeserialize []byte) Message {
	buf := bytes.NewBuffer(toDeserialize)
	newMsg := &ProtoHandshakeMessage{}
	reader := io.Reader(buf)
	if err := binary.Read(reader, binary.BigEndian, newMsg); err != nil {
		panic(err)
	}
	return newMsg
}
