package serialization

import (
	"encoding/binary"
	"net"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func SerializePeer(p peer.Peer) []byte {

	peerBytes := []byte{}

	hostSizeBytes := make([]byte, 4)
	hostBytes := []byte(p.Addr().String())
	binary.BigEndian.PutUint32(hostSizeBytes[0:4], uint32(len(hostBytes)))
	peerBytes = append(peerBytes, hostSizeBytes...)
	peerBytes = append(peerBytes, hostBytes...)

	return peerBytes
}

func DeserializePeer(buf []byte) (int, peer.Peer) {
	addrSize := int(binary.BigEndian.Uint32(buf[0:4]))
	addr := string(buf[4 : 4+addrSize])
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(err)
	}
	return 4 + addrSize, peer.NewPeer(resolved)
}

func SerializePeerArray(peers []peer.Peer) []byte {
	totalBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalBytes, uint32(len(peers)))
	for _, p := range peers {
		totalBytes = append(totalBytes, SerializePeer(p)...)
	}
	return totalBytes
}

func DeserializePeerArray(buf []byte) (int, []peer.Peer) {
	nrPeers := int(binary.BigEndian.Uint32(buf[:4]))
	peers := make([]peer.Peer, nrPeers)
	bufPos := 4
	for i := 0; i < nrPeers; i++ {
		read, peer := DeserializePeer(buf[:bufPos])
		peers[i] = peer
		bufPos += read
	}
	return bufPos, peers
}
