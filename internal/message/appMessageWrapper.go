package message

import (
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/protocol"
)

var appMessageWrapperSerializer = AppMessageWrapperSerializer{}

type AppMessageWrapper struct {
	MessageID       message.ID
	SourceProto     protocol.ID
	DestProto       protocol.ID
	WrappedMsgBytes []byte
}

func NewAppMessageWrapper(
	mId message.ID,
	sourceProto protocol.ID,
	destProto protocol.ID,
	wrappedMsgBytes []byte,
) message.Message {
	return &AppMessageWrapper{
		MessageID:       mId,
		SourceProto:     sourceProto,
		DestProto:       destProto,
		WrappedMsgBytes: wrappedMsgBytes,
	}
}

func (msg *AppMessageWrapper) Type() message.ID {
	return msg.MessageID
}

func (msg *AppMessageWrapper) Serializer() message.Serializer {
	return appMessageWrapperSerializer
}

func (msg *AppMessageWrapper) Deserializer() message.Deserializer {
	return appMessageWrapperSerializer
}

// serializer

type AppMessageWrapperSerializer struct{}

func (serializer AppMessageWrapperSerializer) Serialize(toSerialize message.Message) []byte {
	wrapperMsg := toSerialize.(*AppMessageWrapper)
	msgSize := 2 + 2 + 2
	buf := make([]byte, msgSize)
	binary.BigEndian.PutUint16(buf, wrapperMsg.MessageID)
	binary.BigEndian.PutUint16(buf[2:], wrapperMsg.SourceProto)
	binary.BigEndian.PutUint16(buf[4:], wrapperMsg.DestProto)
	return append(buf, wrapperMsg.WrappedMsgBytes...)
}

func (serializer AppMessageWrapperSerializer) Deserialize(buf []byte) message.Message {
	msg := &AppMessageWrapper{}
	msg.MessageID = binary.BigEndian.Uint16(buf)
	msg.SourceProto = binary.BigEndian.Uint16(buf[2:])
	msg.DestProto = binary.BigEndian.Uint16(buf[4:])
	msg.WrappedMsgBytes = buf[6:]
	return msg
}
