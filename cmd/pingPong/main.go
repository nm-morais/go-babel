package main

import (
	"flag"
	"fmt"
	. "github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/examples/pingPong"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/transport"
	"net"
	"time"
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
		HeartbeatTickDuration: 1 * time.Second,
		ConnectionReadTimeout: 3 * time.Second,
	}
	listener := transport.NewTCPListener(listenAddr)
	pkg.InitProtoManager(configs, listener)

	contactNodeAddr, err := net.ResolveTCPAddr("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}

	pkg.RegisterProtocol(pingPong.NewPingPongProtocol(peer.NewPeer(contactNodeAddr)))
	pkg.Start()
}
