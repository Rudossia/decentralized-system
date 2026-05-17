// p2p-node/internal/core/dht/node_test.go
package dht

import (
	"testing"
	"time"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

func TestNewNode(t *testing.T) {
	self := testPeer(testID(0x01))
	self.SetAddresses([]string{"/ip4/127.0.0.1/tcp/9000"})

	node, err := NewNode(self, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode() error: %v", err)
	}

	if node.Self().ID != self.ID {
		t.Fatalf("unexpected self ID")
	}

	if node.RoutingTable() == nil {
		t.Fatalf("expected routing table")
	}

	if node.Store() == nil {
		t.Fatalf("expected store")
	}
}

func TestNodeHandlePingUpdatesRoutingTable(t *testing.T) {
	node := newTestNode(t, testID(0x01))

	from := testPeer(testID(0x80))

	self, err := node.HandlePing(from)
	if err != nil {
		t.Fatalf("HandlePing() error: %v", err)
	}

	if self == nil {
		t.Fatal("expected self peer in response")
	}

	if !node.RoutingTable().HasPeer(from.ID) {
		t.Fatalf("from peer must be inserted into routing table")
	}
}

func TestNodeHandleFindNodeReturnsClosestPeers(t *testing.T) {
	node := newTestNode(t, testID(0x01))

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testIDLast(0x02))
	p3 := testPeer(testIDLast(0x01))

	node.InsertPeer(p1)
	node.InsertPeer(p2)
	node.InsertPeer(p3)

	closest, err := node.HandleFindNode(testPeer(testID(0x40)), model.NodeID{}, 2)
	if err != nil {
		t.Fatalf("HandleFindNode() error: %v", err)
	}

	requirePeerIDs(t, closest, []model.NodeID{
		p3.ID,
		p2.ID,
	})
}

func TestNodeHandleStoreValueAndFindValue(t *testing.T) {
	node := newTestNode(t, testID(0x01))

	from := testPeer(testID(0x80))
	key := testIDLast(0x55)

	if err := node.HandleStoreValue(from, key, []byte("secret"), from.ID, 1); err != nil {
		t.Fatalf("HandleStoreValue() error: %v", err)
	}

	result, err := node.HandleFindValue(from, key, 3)
	if err != nil {
		t.Fatalf("HandleFindValue() error: %v", err)
	}

	if !result.Found {
		t.Fatal("expected value to be found")
	}

	if string(result.Value) != "secret" {
		t.Fatalf("unexpected value: got %q", result.Value)
	}
}

func TestNodeHandleStorePeerRecordAndFindValue(t *testing.T) {
	node := newTestNode(t, testID(0x01))

	from := testPeer(testID(0x80))

	record := model.NewPeerRecord(
		from.ID,
		[]string{"/ip4/127.0.0.1/tcp/9001"},
		[]byte{0x01},
		[]string{"dht", "tcp"},
		1,
		"peer-agent",
		1,
		time.Hour,
	)

	if err := node.HandleStorePeerRecord(from, record); err != nil {
		t.Fatalf("HandleStorePeerRecord() error: %v", err)
	}

	result, err := node.HandleFindValue(from, from.ID, 3)
	if err != nil {
		t.Fatalf("HandleFindValue() error: %v", err)
	}

	if !result.Found {
		t.Fatal("expected peer record to be found")
	}

	if result.PeerRecord == nil {
		t.Fatal("expected PeerRecord")
	}

	if result.PeerRecord.NodeID != from.ID {
		t.Fatalf("unexpected PeerRecord NodeID")
	}
}

func TestNodeHandleFindValueReturnsClosestWhenNotFound(t *testing.T) {
	node := newTestNode(t, testID(0x01))

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testIDLast(0x01))

	node.InsertPeer(p1)
	node.InsertPeer(p2)

	key := testIDLast(0x55)

	result, err := node.HandleFindValue(testPeer(testID(0x40)), key, 2)
	if err != nil {
		t.Fatalf("HandleFindValue() error: %v", err)
	}

	if result.Found {
		t.Fatal("did not expect value to be found")
	}

	if len(result.Closest) != 2 {
		t.Fatalf("expected 2 closest peers, got %d", len(result.Closest))
	}
}

func TestNodePublishSelfRecord(t *testing.T) {
	self := testPeer(testID(0x01))
	self.SetAddresses([]string{"/ip4/127.0.0.1/tcp/9000"})
	self.SetCapabilities([]string{"dht", "tcp"})

	node, err := NewNode(self, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode() error: %v", err)
	}

	record, err := node.PublishSelfRecord(1)
	if err != nil {
		t.Fatalf("PublishSelfRecord() error: %v", err)
	}

	if record.NodeID != self.ID {
		t.Fatalf("unexpected self record NodeID")
	}

	stored, ok := node.LocalPeerRecord(self.ID)
	if !ok {
		t.Fatal("expected self record to be stored locally")
	}

	if stored.NodeID != self.ID {
		t.Fatalf("unexpected stored record NodeID")
	}
}

func newTestNode(t *testing.T, id model.NodeID) *Node {
	t.Helper()

	self := testPeer(id)
	self.SetAddresses([]string{"/ip4/127.0.0.1/tcp/9000"})
	self.SetCapabilities([]string{"dht", "tcp"})

	cfg := DefaultConfig()
	cfg.K = 8
	cfg.Alpha = 3

	node, err := NewNode(self, cfg)
	if err != nil {
		t.Fatalf("NewNode() error: %v", err)
	}

	return node
}
