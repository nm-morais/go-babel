package hyparview

import (
	"github.com/nm-morais/go-babel/pkg/message"
)

const JoinMessageType = 2001

type JoinMessage struct{}
type joinMessageSerializer struct{}

var defaultJoinMessageSerializer = joinMessageSerializer{}

func (JoinMessage) Type() message.ID                                      { return JoinMessageType }
func (JoinMessage) Serializer() message.Serializer                        { return defaultJoinMessageSerializer }
func (JoinMessage) Deserializer() message.Deserializer                    { return defaultJoinMessageSerializer }
func (joinMessageSerializer) Serialize(msg message.Message) []byte        { return []byte{} }
func (joinMessageSerializer) Deserialize(msgBytes []byte) message.Message { return JoinMessage{} }

const ForwardJoinMessageType = 2002

type ForwardJoinMessage struct{}
type forwardJoinMessageSerializer struct{}

var defaultForwardJoinMessageSerializer = forwardJoinMessageSerializer{}

func (ForwardJoinMessage) Type() message.ID                               { return ForwardJoinMessageType }
func (ForwardJoinMessage) Serializer() message.Serializer                 { return defaultForwardJoinMessageSerializer }
func (ForwardJoinMessage) Deserializer() message.Deserializer {
	return defaultForwardJoinMessageSerializer
}
func (forwardJoinMessageSerializer) Serialize(msg message.Message) []byte { return []byte{} }
func (forwardJoinMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	return ForwardJoinMessage{}
}
