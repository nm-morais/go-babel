package connection

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"time"
)

type TCPConnectionManager struct {

}

func (d TCPConnectionManager) AwaitConnections(listenAddr string) {
	l, err := net.Listen("tcp4", listenAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Error("Connection error:  %+v", err)
		}
	}()
	rand.Seed(time.Now().Unix())

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		d.protocol.OutConnEstablished()
		go d.HandleConn(c)
	}
}
