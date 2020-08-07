package message

const HeartbeatMessageType ID = 1

type HeartbeatMessage struct{}
type HeartbeatSerializer struct{}

var heartbeatSerializer = HeartbeatSerializer{}

func (HeartbeatSerializer) Serialize(_ Message) []byte   { return []byte{} }
func (HeartbeatSerializer) Deserialize(_ []byte) Message { return HeartbeatMessage{} }

func (HeartbeatMessage) Type() ID                       { return HeartbeatMessageType }
func (msg HeartbeatMessage) Serializer() Serializer     { return heartbeatSerializer }
func (msg HeartbeatMessage) Deserializer() Deserializer { return heartbeatSerializer }
