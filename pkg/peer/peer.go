package peer

import (
	"encoding/binary"
	"fmt"
	"net"
)

type Peer interface {
	IP() net.IP
	ProtosPort() uint16
	AnalyticsPort() uint16
	Equals(other Peer) bool
	SerializeToBinary() []byte
	ToString() string
}

type peer struct {
	ip            net.IP
	protosPort    uint16
	analyticsPort uint16
}

func NewPeer(ip net.IP, protosPort uint16, analyticsPort uint16) Peer {
	return &peer{
		ip:            ip,
		protosPort:    protosPort,
		analyticsPort: analyticsPort,
	}
}
func (p *peer) IP() net.IP {
	return p.ip
}

func (p *peer) ProtosPort() uint16 {
	return p.protosPort
}

func (p *peer) AnalyticsPort() uint16 {
	return p.analyticsPort
}

func (p *peer) ToString() string {

	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s:%d", p.ip, p.protosPort)
}

func (p *peer) Equals(otherPeer Peer) bool {

	if p == nil {
		return false
	}

	if otherPeer == nil {
		return false
	}

	return p.ToString() == otherPeer.ToString()
}

func (p peer) SerializeToBinary() []byte {
	peerBytes := make([]byte, 8)
	for idx, b := range p.IP().To4() {
		peerBytes[idx] = b
	}
	binary.BigEndian.PutUint16(peerBytes[4:], p.ProtosPort())
	binary.BigEndian.PutUint16(peerBytes[6:], p.AnalyticsPort())
	return peerBytes
}

func DeserializePeer(buf []byte) (int, Peer) {
	peer := NewPeer(buf[0:4], binary.BigEndian.Uint16(buf[4:6]), binary.BigEndian.Uint16(buf[6:8]))
	return 8, peer
}

func SerializePeerArray(peers []Peer) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, p.SerializeToBinary()...)
	}
	return totalBytes
}

func DeserializePeerArray(buf []byte) (int, []Peer) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]Peer, nrPeers)
	bufPos := 4
	for i := 0; i < nrPeers; i++ {
		read, peer := DeserializePeer(buf[bufPos:])
		peers[i] = peer
		bufPos += read
	}
	return bufPos, peers
}
