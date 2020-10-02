package messageIO

import (
	"encoding/binary"
	"io"
)

type MessageReader interface {
	io.ReadCloser
}

type messageReader struct {
	stream io.ReadCloser
	buf    []byte
}

func NewMessageReader(readCLoser io.ReadCloser) MessageReader {
	return &messageReader{stream: readCLoser, buf: make([]byte, 2048)}
}

func (a *messageReader) Close() error {
	a.buf = nil
	return a.stream.Close()
}

func (a *messageReader) Read(msgBytes []byte) (int, error) {

	msgSizeBytes := make([]byte, 4)
	_, err := io.ReadFull(a.stream, msgSizeBytes)
	if err != nil {
		return 0, err
	}
	msgSize := int(binary.BigEndian.Uint32(msgSizeBytes))
	if len(msgBytes) < msgSize {
		msgBytes = append(msgBytes, make([]byte, msgSize-len(msgBytes))...)
	}
	return io.ReadFull(a.stream, msgBytes[:msgSize])

	/*
		for {
			var read int
			var err error

			read, err = a.reader.Read(a.buf)
			if err != nil {
				if err != io.EOF {
					return read, err
				} else {
					if read == 0 && len(a.carry) == 0 {
						//log.Info("Returning with no bytes in a.carry")
						return read, err
					}
				}
			}

			if len(a.carry) > 0 {
				read += len(a.carry)
				a.buf = append(a.carry, a.buf...)
				a.carry = []byte{}
			}

			//log.Infof("Have %d bytes of a.carry", len(a.carry))
			if read <= 4 { // this case assures that there are at least 4 bytes to read a messageSize
				//log.Info("Not enough bytes to read messageSize")
				a.carry = a.buf[:read]
				continue
			}

			bufPos := 0
			for bufPos < read {
				if bufPos != 0 {
					//log.Warn("Processing remaining bytes of message")
				}
				msgSize := int(binary.BigEndian.Uint32(a.buf[bufPos : bufPos+4]))
				bufPos += 4
				//log.Info("read: ", read)
				//log.Info("msgSize: ", msgSize)
				//log.Info("bufPos: ", bufPos)
				if bufPos+msgSize <= read {
					//log.Info("Read message: ", string(a.buf[bufPos:bufPos+msgSize]))
					if read > bufPos+msgSize {
						a.carry = a.buf[bufPos+msgSize : read]
						//	log.Info("Returning but have remaining bytes in carry", a.carry)
					}
					return copy(msgBytes, a.buf[bufPos:bufPos+msgSize]), nil
				} else {
					//log.Warn(" have leftover but do not have enough bytes to read msgBody")
					a.carry = a.buf[bufPos-4 : read]
					continue
				}
			}
		}
	*/
}
