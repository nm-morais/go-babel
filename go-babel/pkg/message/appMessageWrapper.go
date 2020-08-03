package message

import (
	"bytes"
	"encoding/binary"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"io"
)

type protoMessageMetadata struct {
	MessageID   ID
	SourceProto protocol.ID
	DestProtos  []protocol.ID
}

type AppMessageWrapper struct {
	WrappedMessage Message
	Metadata       protoMessageMetadata
}

func NewAppMessageWrapper(sourceProto protocol.ID, destProtos []protocol.ID, msg Message) Message {
	return &AppMessageWrapper{
		Metadata: protoMessageMetadata{
			MessageID:   msg.Type(),
			SourceProto: sourceProto,
			DestProtos:  destProtos,
		},
		WrappedMessage: msg,
	}
}

func (msg *AppMessageWrapper) Type() ID {
	return msg.WrappedMessage.Type()
}

func (msg *AppMessageWrapper) Serialize(buf *bytes.Buffer) {
	writer := io.Writer(buf)
	if err := binary.Write(writer, binary.BigEndian, msg.Metadata); err != nil {
		panic(err)
	}
	msg.WrappedMessage.Serialize(buf)
}

func (msg *AppMessageWrapper) Deserialize(buf *bytes.Buffer) Message {
	reader := io.Reader(buf)
	msgMetadata := protoMessageMetadata{}
	if err := binary.Read(reader, binary.BigEndian, &msgMetadata); err != nil {
		panic(err)
	}
	msg.Metadata = msgMetadata
	msg.WrappedMessage = NewGenericWrapperMessage(msgMetadata.MessageID, buf.Bytes())
	return msg
}
