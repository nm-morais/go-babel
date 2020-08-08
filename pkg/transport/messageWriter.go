package transport

import (
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"io"
)

type MessageWriter struct {
	w io.Writer
}

func NewMessageWriter(w io.Writer) MessageWriter {
	return MessageWriter{
		w: w,
	}
}

func (w MessageWriter) Write(p []byte) (int, error) {
	log.Infof("MessageWriter writing...")
	msgSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgSizeBytes, uint32(len(p)))
	return w.w.Write(append(msgSizeBytes, p...))
}
