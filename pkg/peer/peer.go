package peer

import (
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
)

type Peer interface {
	IP() net.IP
	ProtosPort() uint16
	AnalyticsPort() uint16
	String() string
	Marshal() []byte
	Unmarshal([]byte) int
}

type IPeer struct {
	ip            net.IP
	protosPort    uint16
	analyticsPort uint16
}

func NewPeer(ip net.IP, protosPort uint16, analyticsPort uint16) *IPeer {
	return &IPeer{
		ip:            ip,
		protosPort:    protosPort,
		analyticsPort: analyticsPort,
	}
}
func (p *IPeer) IP() net.IP {
	return p.ip
}

func (p *IPeer) ProtosPort() uint16 {
	return p.protosPort
}

func (p *IPeer) AnalyticsPort() uint16 {
	return p.analyticsPort
}

func (p *IPeer) String() string {

	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s:%d", p.ip, p.protosPort)
}

func PeersEqual(p1 Peer, p2 Peer) bool {

	if reflect.ValueOf(p1).IsZero() {
		return false
	}

	if reflect.ValueOf(p2).IsZero() {
		return false
	}

	return p1.IP().Equal(p2.IP()) && p1.AnalyticsPort() == p2.AnalyticsPort() && p1.ProtosPort() == p2.ProtosPort()
}

func (p *IPeer) Marshal() []byte {
	peerBytes := make([]byte, 8)
	for idx, b := range p.IP().To4() {
		peerBytes[idx] = b
	}
	binary.BigEndian.PutUint16(peerBytes[4:], p.ProtosPort())
	binary.BigEndian.PutUint16(peerBytes[6:], p.AnalyticsPort())
	return peerBytes
}

func (p *IPeer) Unmarshal(buf []byte) int {

	p.ip = buf[0:4]
	p.protosPort = binary.BigEndian.Uint16(buf[4:6])
	p.analyticsPort = binary.BigEndian.Uint16(buf[6:8])
	return 8
}

func SerializePeerArray(peers []Peer) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, p.Marshal()...)
	}
	return totalBytes
}

func DeserializePeerArray(buf []byte) (int, []Peer) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]Peer, nrPeers)
	bufPos := 4
	for i := 0; i < nrPeers; i++ {
		p := &IPeer{}
		bufPos += p.Unmarshal(buf[bufPos:])
		peers[i] = p
	}
	return bufPos, peers
}
