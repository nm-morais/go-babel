package message

import (
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
)

var appMessageWrapperSerializer = AppMessageWrapperSerializer{}

type AppMessageWrapper struct {
	Sender          peer.Peer
	MessageID       message.ID
	SourceProto     protocol.ID
	DestProto       protocol.ID
	WrappedMsgBytes []byte
}

func NewAppMessageWrapper(
	mID message.ID,
	sourceProto protocol.ID,
	destProto protocol.ID,
	sender peer.Peer,
	wrappedMsgBytes []byte,
) message.Message {
	return &AppMessageWrapper{
		MessageID:       mID,
		SourceProto:     sourceProto,
		DestProto:       destProto,
		WrappedMsgBytes: wrappedMsgBytes,
		Sender:          sender,
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
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf, wrapperMsg.MessageID)
	binary.BigEndian.PutUint16(buf[2:], wrapperMsg.SourceProto)
	binary.BigEndian.PutUint16(buf[4:], wrapperMsg.DestProto)
	buf = append(buf, wrapperMsg.Sender.Marshal()...)
	return append(buf, wrapperMsg.WrappedMsgBytes...)
}

func (serializer AppMessageWrapperSerializer) Deserialize(buf []byte) message.Message {
	msg := &AppMessageWrapper{}
	msg.MessageID = binary.BigEndian.Uint16(buf)
	msg.SourceProto = binary.BigEndian.Uint16(buf[2:])
	msg.DestProto = binary.BigEndian.Uint16(buf[4:])
	p := &peer.IPeer{}
	p.Unmarshal(buf[6:])
	msg.WrappedMsgBytes = buf[14:]
	msg.Sender = p
	return msg
}
