package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
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
	var cpuprofile string

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")
	flag.Parse()

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			fmt.Println("Sig:", sig)
			pprof.StopCPUProfile()
			os.Exit(0)
		}
	}()

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	protoManagerConf := pkg.ProtocolManagerConfig{
		LogFolder:        "/Users/nunomorais/go/src/github.com/nm-morais/go-babel/logs/",
		HandshakeTimeout: 1 * time.Second,
		DialTimeout:      1 * time.Second,
		Peer:             peer.NewPeer(net.IPv4(127, 0, 0, 1), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	fmt.Println("Self peer: ", protoManagerConf.Peer.String())
	nodeWatcherConf := pkg.NodeWatcherConf{
		MaxRedials:              3,
		HbTickDuration:          1 * time.Second,
		NewLatencyWeight:        0.1,
		NrTestMessagesToSend:    3,
		NrTestMessagesToReceive: 1,
		OldLatencyWeight:        0.9,
		TcpTestTimeout:          5 * time.Second,
		UdpTestTimeout:          5 * time.Second,
		WindowSize:              5,
	}
	contactNode := peer.NewPeer(net.IPv4(127, 0, 0, 1), uint16(1200), uint16(1201))

	fmt.Println(fmt.Sprintf("Contact node IP %s", contactNode.IP()))
	fmt.Println(fmt.Sprintf("Contact node %+v", contactNode))

	pkg.InitProtoManager(protoManagerConf)
	pkg.RegisterListenAddr(&net.TCPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())})
	pkg.RegisterListenAddr(&net.UDPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())})
	pkg.InitNodeWatcher(nodeWatcherConf)
	pkg.RegisterProtocol(NewHyparviewProtocol(contactNode))
	pkg.Start()
}
