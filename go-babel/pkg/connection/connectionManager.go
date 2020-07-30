package connection

import "github.com/DeMMon/go-babel/pkg/peer"

type Manager interface {
	AwaitConnections(listenAddr string)
	GetConnection(peer peer.Peer) Connection
}
