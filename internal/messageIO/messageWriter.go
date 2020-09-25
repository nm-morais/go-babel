package messageIO

import (
	"encoding/binary"
	"io"
)

type MessageWriter interface {
	io.WriteCloser
}

type messageWriter struct {
	outStream io.WriteCloser
}

func NewMessageWriter(w io.WriteCloser) MessageWriter {
	return &messageWriter{
		outStream: w,
	}
}

func (w *messageWriter) Close() error {
	//log.Infof("messageWriter writing...")
	return w.outStream.Close()
}

func (w *messageWriter) Write(p []byte) (int, error) {
	//log.Infof("messageWriter writing...")
	msgSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgSizeBytes, uint32(len(p)))
	return w.outStream.Write(append(msgSizeBytes, p...))
}
