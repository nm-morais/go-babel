package message

import (
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
)

const HandshakeMessageID = 1

type ProtoHandshakeMessage struct {
	Peer        peer.Peer
	DialerProto protocol.ID
	TunnelType  uint8
}

func NewProtoHandshakeMessage(dialerProto protocol.ID, p peer.Peer, temporaryConn uint8) *ProtoHandshakeMessage {
	return &ProtoHandshakeMessage{
		DialerProto: dialerProto,
		Peer:        p,
		TunnelType:  temporaryConn,
	}
}

func (msg ProtoHandshakeMessage) Type() message.ID {
	return HandshakeMessageID
}

type ProtoHandshakeMessageSerializer struct{}

func (msg *ProtoHandshakeMessage) Serialize() []byte {
	msgSize := 3
	buf := make([]byte, msgSize)
	bufPos := 0
	buf[0] = msg.TunnelType
	bufPos++
	binary.BigEndian.PutUint16(buf[bufPos:], msg.DialerProto)
	toSend := append(buf, msg.Peer.Marshal()...)
	return toSend
}

// func (msg *ProtoHandshakeMessage) Deserialize(buf *bytes.Buffer) error {
// 	if buf.Len() < 11 {
// 		return ErrNotEnoughLen
// 	}
// 	b, err := buf.ReadByte()
// 	if err != nil {
// 		return err
// 	}
// 	msg.TunnelType = b
// 	err = binary.Read(buf, binary.BigEndian, &msg.DialerProto)
// 	if err != nil {
// 		return err
// 	}
// 	p := &peer.IPeer{}
// 	p.UnmarshalFromBuf(buf)
// 	msg.Peer = p
// 	return nil
// }

func (msg *ProtoHandshakeMessage) Deserialize(buf []byte) error {
	if len(buf) < 11 {
		return ErrNotEnoughLen
	}
	bufPos := 0
	msg.TunnelType = buf[0]
	bufPos++
	dialerProto := binary.BigEndian.Uint16(buf[bufPos:])
	bufPos += 2

	p := &peer.IPeer{}
	p.Unmarshal(buf[bufPos:])
	msg.Peer = p
	msg.DialerProto = dialerProto
	return nil
}
