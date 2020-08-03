package message

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/protocol"
)

type ProtoHandshakeMessage struct {
	Protos []protocol.ID
}

func NewProtoHandshakeMessage(protos []protocol.ID) message.Message {
	return &ProtoHandshakeMessage{
		Protos: protos,
	}
}

func (msg *ProtoHandshakeMessage) Type() message.ID {
	panic("implement me")
}

func (msg *ProtoHandshakeMessage) Serialize(buf *bytes.Buffer) {
	writer := bufio.NewWriter(buf)
	if err := binary.Write(writer, binary.BigEndian, msg); err != nil {
		panic(err)
	}
}

func (msg *ProtoHandshakeMessage) Deserialize(buf *bytes.Buffer) message.Message {
	writer := bufio.NewReader(buf)
	if err := binary.Read(writer, binary.BigEndian, msg); err != nil {
		panic(err)
	}
	return msg
}
