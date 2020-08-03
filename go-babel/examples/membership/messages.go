package membership

import (
	"bytes"
	"github.com/DeMMon/go-babel/pkg/message"
)

const pingType message.ID = 1001
const pongType message.ID = 1002

type Ping struct {
	payload string
}

func (Ping) Type() message.ID {
	return pingType
}

func (Ping) Serialize(buf *bytes.Buffer) {
	buf.WriteString("ping")
}

func (msg Ping) Deserialize(buf *bytes.Buffer) message.Message {
	msgBytes := buf.Bytes()
	msg.payload = string(msgBytes)
	return msg
}

type Pong struct {
	payload string
}

func (Pong) Type() message.ID {
	return pongType
}

func (Pong) Serialize(buf *bytes.Buffer) {
	buf.WriteString("pong")
}

func (msg Pong) Deserialize(buf *bytes.Buffer) message.Message {
	msgBytes := buf.Bytes()
	msg.payload = string(msgBytes)
	return msg
}
