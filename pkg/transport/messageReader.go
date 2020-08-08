package transport

import (
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"io"
)

type MessageReader struct {
	reader io.Reader
}

func NewMessageReader(reader io.Reader) MessageReader {
	return MessageReader{reader: reader}
}

func (a MessageReader) Read(msgBytes []byte) (int, error) {
	msgSizebytes := make([]byte, 4)
	log.Infof("MessageReader reading...")
	_, err := io.ReadFull(a.reader, msgSizebytes)
	if err != nil {
		return 0, err
	}
	log.Infof("MessageReader reading...")
	msgSize := int(binary.BigEndian.Uint32(msgSizebytes))
	return io.ReadFull(a.reader, msgBytes[msgSize:])
}
