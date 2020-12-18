package main

import (
	"flag"
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
		<-c
		pprof.StopCPUProfile()
		os.Exit(0)
	}()

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	protoManagerConf := pkg.Config{

		SmConf: pkg.StreamManagerConf{
			DialTimeout: 3 * time.Second,
		},
		LogFolder:        "/Users/nunomorais/go/src/github.com/nm-morais/go-babel/logs/",
		HandshakeTimeout: 1 * time.Second,
		Peer:             peer.NewPeer(net.IPv4(127, 0, 0, 1), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	contactNode := peer.NewPeer(net.IPv4(127, 0, 0, 1), uint16(1200), uint16(1201))

	p := pkg.NewProtoManager(protoManagerConf)
	p.RegisterListenAddr(&net.TCPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())})
	p.RegisterListenAddr(&net.UDPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())})
	p.RegisterProtocol(NewHyparviewProtocol(contactNode, p))
	p.StartSync()
}
