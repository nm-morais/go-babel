package message

import (
	"bytes"
)

const MaxMessageBytes = 2048

type ID uint16

type Message interface {
	Type() ID
	Serialize(buf *bytes.Buffer)
	Deserialize(buf *bytes.Buffer) Message
}
