package protocol

import "github.com/nm-morais/go-babel/pkg/peer"

type ID = uint16

type Protocol interface {
	ID() ID
	Init()
	Start()

	// peer events
	InConnRequested(peer peer.Peer) bool             // if true, will subscribe protocol to connectionEvents
	DialSuccess(sourceProto ID, peer peer.Peer) bool // if true, will subscribe protocol to connectionEvents

	DialFailed(peer peer.Peer)
	PeerDown(peer peer.Peer)
}
