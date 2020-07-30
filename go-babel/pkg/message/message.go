package message

import (
	. "github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"github.com/DeMMon/go-babel/pkg/utils"
)

type Message interface {
	ID() ID
	Type() MessageType
	Serialize() []byte
	Deserialize(msgBytes []byte) Message
}

type Wrapper struct {
	ProtocolID   ID
	MessageID    ID
	messageBytes []byte
}

func NewMessageWrapper(protocol protocol.Protocol, message Message) *Wrapper {
	return &Wrapper{
		ProtocolID:   protocol.ID(),
		MessageID:    message.ID(),
		messageBytes: message.Serialize(),
	}
}

func (mw *Wrapper) ID() ID {
	return mw.MessageID
}

func (mw *Wrapper) Protocol() ID {
	return mw.ProtocolID
}

func (mw *Wrapper) Serialize() []byte {
	return utils.SerializeToJson(mw)
}

func (mw *Wrapper) Deserialize(msgBytes []byte) Wrapper {
	messageWrapper := Wrapper{}
	utils.DeserializeFromJson(msgBytes, messageWrapper)
	return messageWrapper
}
