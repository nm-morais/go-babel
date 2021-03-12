package serializationUtils

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func EncodeString(s string) []byte {
	b := new(bytes.Buffer)
	EncodeStringToBuffer(s, b)
	return b.Bytes()
}

func DecodeString(buf []byte) string {
	b := bytes.NewBuffer(buf)
	return DecodeStringFromBuffer(b)
}

func EncodeStringToBuffer(s string, buffer *bytes.Buffer) {
	sb := []byte(s)
	err := binary.Write(buffer, binary.BigEndian, uint16(len(sb)))
	if err != nil {
		panic(err)
	}
	n, err := buffer.Write(sb)
	if n != len(sb) {
		panic(fmt.Sprint("Expected to write", len(sb), "wrote", n))
	}

	if err != nil {
		panic(err)
	}
}

func DecodeStringFromBuffer(buffer *bytes.Buffer) string {
	var sLen uint16
	err := binary.Read(buffer, binary.BigEndian, &sLen)
	if err != nil {
		panic(err)
	}

	sb := make([]byte, sLen)
	n, err := buffer.Read(sb)
	if n != int(sLen) {
		panic(fmt.Sprint("Expected to read", sLen, "read", n))
	}

	if err != nil {
		panic(err)
	}

	return string(sb)
}

func EncodeNumberToBuffer(n interface{}, buffer *bytes.Buffer) {
	err := binary.Write(buffer, binary.BigEndian, n)
	if err != nil {
		panic(err)
	}
}

func DecodeNumberFromBuffer(nPointer interface{}, buffer *bytes.Buffer) {
	err := binary.Read(buffer, binary.BigEndian, nPointer)
	if err != nil {
		panic(err)
	}
}
