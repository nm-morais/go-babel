package heartbeat

import (
	"github.com/nm-morais/go-babel/pkg/message"
)

const HeartbeatMessageType message.ID = 1

type HeartbeatMessage struct{}
type HeartbeatSerializer struct{}

var pingSerializer = HeartbeatSerializer{}

func (HeartbeatSerializer) Serialize(_ message.Message) []byte   { return []byte{} }
func (HeartbeatSerializer) Deserialize(_ []byte) message.Message { return HeartbeatMessage{} }

func (HeartbeatMessage) Type() message.ID                       { return HeartbeatMessageType }
func (msg HeartbeatMessage) Serializer() message.Serializer     { return pingSerializer }
func (msg HeartbeatMessage) Deserializer() message.Deserializer { return pingSerializer }
