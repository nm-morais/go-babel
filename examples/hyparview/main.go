package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/stream"
)

func main() {
	rand.Seed(time.Now().Unix() + rand.Int63())
	minPort := 8000
	maxPort := 9000

	var portVar int
	var randPort bool

	flag.IntVar(&portVar, "p", 1200, "port")
	flag.BoolVar(&randPort, "r", false, "port")
	flag.Parse()

	if randPort {
		portVar = rand.Intn(maxPort-minPort) + minPort
	}
	listenAddrTcp, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", portVar))
	if err != nil {
		panic(err)
	}
	listenAddrUdp, err := net.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", portVar))
	if err != nil {
		panic(err)
	}
	config := configs.ProtocolManagerConfig{
		LogFolder:             "/Users/nunomorais/go/src/github.com/nm-morais/go-babel/logs/",
		HandshakeTimeout:      1 * time.Second,
		HeartbeatTickDuration: 1 * time.Second,
		DialTimeout:           1 * time.Second,
		ConnectionReadTimeout: 5 * time.Second,
	}
	pkg.InitProtoManager(config, listenAddrTcp)
	contactNodeAddr, err := net.ResolveTCPAddr("tcp", "localhost:1200")
	if err != nil {
		panic(err)
	}

	pkg.RegisterListener(stream.NewTCPListener(listenAddrTcp))
	pkg.RegisterListener(stream.NewUDPListener(listenAddrUdp))
	pkg.RegisterProtocol(NewHyparviewProtocol(peer.NewPeer(contactNodeAddr)))

	pkg.Start()
}
