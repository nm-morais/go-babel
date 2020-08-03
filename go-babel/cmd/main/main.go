package main

import (
	"github.com/nm-morais/DeMMon/go-babel/examples/pingPong"
	"github.com/nm-morais/DeMMon/go-babel/pkg"
	"github.com/nm-morais/DeMMon/go-babel/pkg/peer"
	"github.com/nm-morais/DeMMon/go-babel/pkg/transport"
	"net"
)

func main() {
	pkg.InitProtoManager()

	listenAddr, err := net.ResolveTCPAddr("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}
	listener := transport.NewTCPListener(listenAddr)
	pkg.RegisterTransportListener(listener)

	contactNodeAddr, err := net.ResolveTCPAddr("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}
	pkg.RegisterProtocol(pingPong.NewPingPongProtocol(peer.NewPeer(contactNodeAddr), peer.NewPeer(listenAddr)))
}
