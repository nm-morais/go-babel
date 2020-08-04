package main

import (
	"flag"
	"fmt"
	. "github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/examples/heartbeat"
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

	listenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", flagvar))
	if err != nil {
		panic(err)
	}
	configs := ProtocolManagerConfig{
		ListenAddr: listenAddr,
	}
	pkg.InitProtoManager(configs)

	listener := transport.NewTCPListener(listenAddr)
	pkg.RegisterTransportListener(listener)

	contactNodeAddr, err := net.ResolveTCPAddr("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}

	pkg.RegisterProtocol(&heartbeat.Heartbeat{})
	pkg.RegisterProtocol(pingPong.NewPingPongProtocol(peer.NewPeer(contactNodeAddr)))
	pkg.Start()
}
