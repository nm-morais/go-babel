package message

import (
	"bytes"
	"encoding/binary"
	"github.com/nm-morais/DeMMon/go-babel/pkg/protocol"
	"github.com/nm-morais/DeMMon/go-babel/pkg/serialization"
	"io"
)

var appMessageWrapperSerializer = AppMessageWrapperSerializer{}

type AppMessageWrapper struct {
	MessageID       ID
	SourceProto     protocol.ID
	DestProtos      []protocol.ID
	WrappedMsgBytes []byte
}

func NewAppMessageWrapper(mId ID, sourceProto protocol.ID, destProtos []protocol.ID, wrappedMsgBytes []byte) Message {
	return &AppMessageWrapper{
		MessageID:       mId,
		SourceProto:     sourceProto,
		DestProtos:      destProtos,
		WrappedMsgBytes: wrappedMsgBytes,
	}
}

func (msg *AppMessageWrapper) Type() ID {
	return msg.MessageID
}

func (msg *AppMessageWrapper) Serializer() serialization.Serializer {
	return appMessageWrapperSerializer
}

func (msg *AppMessageWrapper) Deserializer() serialization.Deserializer {
	return appMessageWrapperSerializer
}

// serializer

type AppMessageWrapperSerializer struct{}

func (serializer AppMessageWrapperSerializer) Serialize(toSerialize Message) []byte {
	wrappedMsg := toSerialize.(*AppMessageWrapper)
	buf := &bytes.Buffer{}
	writer := io.Writer(buf)
	if err := binary.Write(writer, binary.BigEndian, wrappedMsg.MessageID); err != nil {
		panic(err)
	}
	if err := binary.Write(writer, binary.BigEndian, wrappedMsg.SourceProto); err != nil {
		panic(err)
	}
	if err := binary.Write(writer, binary.BigEndian, wrappedMsg.DestProtos); err != nil {
		panic(err)
	}
	buf.Write(wrappedMsg.WrappedMsgBytes)
	return buf.Bytes()
}

func (serializer AppMessageWrapperSerializer) Deserialize(toDeserialize []byte) Message {
	buf := bytes.NewBuffer(toDeserialize)
	reader := io.Reader(buf)
	msg := &AppMessageWrapper{}
	if err := binary.Read(reader, binary.BigEndian, msg.MessageID); err != nil {
		panic(err)
	}
	if err := binary.Read(reader, binary.BigEndian, msg.SourceProto); err != nil {
		panic(err)
	}
	if err := binary.Read(reader, binary.BigEndian, msg.DestProtos); err != nil {
		panic(err)
	}
	msg.WrappedMsgBytes = buf.Bytes()
	return msg
}
