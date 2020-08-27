package analytics

import (
	"net"
	"testing"

	"github.com/nm-morais/go-babel/pkg/peer"
)

var testPeer = peer.NewPeer(net.IPv4(0, 0, 0, 0), 1000, 2000)

func Test_serializeHeartbeatMessage(t *testing.T) {
	test := NewTestHBMessage(testPeer)
	bytes := serializeHeartbeatMessage(test)
	test2 := deserializeHeartbeatMessage(bytes)

	if !test.sender.Equals(test2.sender) {
		t.Error("Sender does not match")
		t.Fail()
	}

	if test.IsTest != test2.IsTest {
		t.Error("isTest does not match")
		t.Fail()
	}
}

func Test_serializeHeartbeatMessage2(t *testing.T) {
	test := NewHBMessageForceReply(testPeer)

	bytes := serializeHeartbeatMessage(test)

	test2 := deserializeHeartbeatMessage(bytes)

	t.Logf("%+v", test)
	t.Logf("%+v", test2)

	if !test.sender.Equals(test2.sender) {
		t.Error("Sender does not match")
		t.FailNow()
	}

	if test.IsTest != test2.IsTest {
		t.Error("isTest does not match")
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