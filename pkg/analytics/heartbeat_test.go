package analytics

import (
	"net"
	"testing"

	"github.com/nm-morais/go-babel/pkg/peer"
)

var testPeer = peer.NewPeer(net.IPv4(0, 0, 0, 0), 1000, 2000)

func Test_serializeHeartbeatMessage(t *testing.T) {
	test := NewHBMessageForceReply(testPeer)
	bytes := SerializeHeartbeatMessage(test)
	test2 := DeserializeHeartbeatMessage(bytes)

	if !test.Sender.Equals(test2.Sender) {
		t.Error("Sender does not match")
		t.Fail()
	}
}

func Test_serializeHeartbeatMessage2(t *testing.T) {
	test := NewHBMessageForceReply(testPeer)

	bytes := SerializeHeartbeatMessage(test)

	test2 := DeserializeHeartbeatMessage(bytes)

	t.Logf("%+v", test)
	t.Logf("%+v", test2)

	if !test.Sender.Equals(test2.Sender) {
		t.Error("Sender does not match")
		t.FailNow()
	}

	if test.ForceReply != test2.ForceReply {
		t.Error("ForceReply does not match")
		t.FailNow()
	}

	if !test.TimeStamp.Equal(test2.TimeStamp) {
		t.Error("Timestamp does not match")
		t.FailNow()
	}

	if test.IsReply != test2.IsReply {
		t.Error("isReply does not match")
		t.FailNow()
	}
}
