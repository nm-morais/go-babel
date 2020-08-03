package main

import (
	"flag"
	"fmt"
	"github.com/nm-morais/go-babel/examples/pingPong"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/transport"
	"net"
)

func main() {

	var flagvar int
	flag.IntVar(&flagvar, "p", 1234, "port")
	flag.Parse()

	pkg.InitProtoManager()

	listenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", flagvar))
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
	pkg.Start()
}
