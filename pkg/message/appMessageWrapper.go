package message

import (
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/protocol"
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

func (msg *AppMessageWrapper) Serializer() Serializer {
	return appMessageWrapperSerializer
}

func (msg *AppMessageWrapper) Deserializer() Deserializer {
	return appMessageWrapperSerializer
}

// serializer

type AppMessageWrapperSerializer struct{}

func (serializer AppMessageWrapperSerializer) Serialize(toSerialize Message) []byte {
	wrapperMsg := toSerialize.(*AppMessageWrapper)
	msgSize := 0
	msgSize += 2 + 2 + 2 + 2*len(wrapperMsg.DestProtos)
	buf := make([]byte, msgSize)
	binary.BigEndian.PutUint16(buf, wrapperMsg.MessageID)
	binary.BigEndian.PutUint16(buf[2:], wrapperMsg.SourceProto)
	binary.BigEndian.PutUint16(buf[4:], uint16(len(wrapperMsg.DestProtos)))
	bufPos := 6
	for _, protoID := range wrapperMsg.DestProtos {
		binary.BigEndian.PutUint16(buf[bufPos:], protoID)
		bufPos += 2
	}
	return append(buf, wrapperMsg.WrappedMsgBytes...)
}

func (serializer AppMessageWrapperSerializer) Deserialize(buf []byte) Message {
	msg := &AppMessageWrapper{}
	msg.MessageID = binary.BigEndian.Uint16(buf)
	msg.SourceProto = binary.BigEndian.Uint16(buf[2:])
	nrProtos := int(binary.BigEndian.Uint16(buf[4:]))
	msg.DestProtos = make([]protocol.ID, nrProtos)
	bufPos := 6
	for i := 0; i < nrProtos; i++ {
		msg.DestProtos[i] = binary.BigEndian.Uint16(buf[bufPos:])
		bufPos += 2
	}
	msg.WrappedMsgBytes = buf[bufPos:]
	return msg
}
