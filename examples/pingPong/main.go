package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/stream"
)

func main() {

	var flagvar int
	flag.IntVar(&flagvar, "p", 1234, "port")
	flag.Parse()

	listenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", flagvar))
	if err != nil {
		panic(err)
	}
	configs := configs.ProtocolManagerConfig{
		HeartbeatTickDuration: 1 * time.Second,
		ConnectionReadTimeout: 3 * time.Second,
	}
	pkg.InitProtoManager(configs, listenAddr)

	contactNodeAddr, err := net.ResolveTCPAddr("tcp", "localhost:1234")
	if err != nil {
		panic(err)
	}

	listener := stream.NewTCPListener(listenAddr)
	pkg.RegisterListener(listener)
	pkg.RegisterProtocol(NewPingPongProtocol(peer.NewPeer(contactNodeAddr)))
	pkg.Start()
}
