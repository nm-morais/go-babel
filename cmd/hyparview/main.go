package main

import (
	"flag"
	"fmt"
	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/examples/hyparview"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"time"
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
	log.Info("Local Addr: ", fmt.Sprintf("localhost:%d", portVar))
	listenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", portVar))
	if err != nil {
		panic(err)
	}
	config := configs.ProtocolManagerConfig{
		HandshakeTimeout:      1 * time.Second,
		HeartbeatTickDuration: 1 * time.Second,
		DialTimeout:           1 * time.Second,
		ConnectionReadTimeout: 5 * time.Second,
	}
	pkg.InitProtoManager(config, transport.NewTCPListener(listenAddr))
	contactNodeAddr, err := net.ResolveTCPAddr("tcp", "localhost:1200")
	if err != nil {
		panic(err)
	}

	pkg.RegisterProtocol(hyparview.NewHYparviewProtocol(peer.NewPeer(contactNodeAddr)))
	pkg.Start()
}
