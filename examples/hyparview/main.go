package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/analytics"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/stream"
)

func main() {
	rand.Seed(time.Now().Unix() + rand.Int63())
	minProtosPort := 7000
	maxProtosPort := 8000
	minAnalyticsPort := 8000
	maxAnalyticsPort := 9000

	var protosPortVar int
	var analyticsPortVar int
	var randProtosPort bool
	var randAnalyticsPort bool

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&randProtosPort, "rprotos", false, "port")

	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")
	flag.Parse()

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	protoManagerConf := pkg.ProtocolManagerConfig{
		LogFolder:             "/Users/nunomorais/go/src/github.com/nm-morais/go-babel/logs/",
		HandshakeTimeout:      1 * time.Second,
		HeartbeatTickDuration: 1 * time.Second,
		DialTimeout:           1 * time.Second,
		ConnectionReadTimeout: 5 * time.Second,
		Peer:                  peer.NewPeer(net.IPv4(127, 0, 0, 1), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	fmt.Println("Self peer: ", protoManagerConf.Peer.ToString())

	nodeWatcherConf := analytics.NodeWatcherConf{
		MaxRedials:              3,
		HbTickDuration:          1 * time.Second,
		MinSamplesFaultDetector: 3,
		NewLatencyWeight:        0.1,
		NrTestMessages:          3,
		OldLatencyWeight:        0.9,
		TcpTestTimeout:          5,
		UdpTestTimeout:          5,
		WindowSize:              5,
	}

	contactNode := peer.NewPeer(net.IPv4(127, 0, 0, 1), uint16(1200), uint16(1201))

	fmt.Println(fmt.Sprintf("Contact node IP %s", contactNode.IP()))
	fmt.Println(fmt.Sprintf("Contact node %+v", contactNode))

	pkg.InitProtoManager(protoManagerConf)
	pkg.RegisterListener(stream.NewTCPListener(&net.TCPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())}))
	pkg.RegisterListener(stream.NewUDPListener(&net.UDPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())}))
	pkg.InitNodeWatcher(nodeWatcherConf)
	pkg.RegisterProtocol(NewHyparviewProtocol(contactNode))
	pkg.Start()
}
