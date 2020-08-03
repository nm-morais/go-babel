package message

import (
	"bytes"
)

type GenericMessage struct {
	MsgId    ID
	MSgBytes *bytes.Buffer
}

func NewGenericWrapperMessage(msgId ID, msgBytes []byte) Message {
	return &GenericMessage{MsgId: msgId, MSgBytes: bytes.NewBuffer(msgBytes)}
}

func (m *GenericMessage) Type() ID {
	return m.MsgId
}

func (m *GenericMessage) Serialize(buf *bytes.Buffer) {
	buf.Write(buf.Bytes())
}

func (m *GenericMessage) Deserialize(buf *bytes.Buffer) Message {
	m.MSgBytes = bytes.NewBuffer(buf.Bytes())
	return m
}
