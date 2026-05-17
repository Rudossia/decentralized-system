// p2p-node/internal/core/dht/routing_table_test.go
package dht

import (
	"errors"
	"testing"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

func TestRoutingTableInsertIgnoresSelf(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(2))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	result := rt.Insert(testPeer(selfID))

	if result.Status != InsertIgnoredSelf {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, InsertIgnoredSelf)
	}

	if !errors.Is(result.Err, ErrSelfPeer) {
		t.Fatalf("expected ErrSelfPeer, got %v", result.Err)
	}
}

func TestRoutingTableInsertAddsPeerToExpectedBucket(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(2))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	peer := testPeer(testID(0x80))

	result := rt.Insert(peer)

	if result.Status != InsertAdded {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, InsertAdded)
	}

	if result.BucketIndex != 0 {
		t.Fatalf("unexpected bucket index: got %d, want 0", result.BucketIndex)
	}

	if rt.Len() != 1 {
		t.Fatalf("unexpected routing table len: got %d, want 1", rt.Len())
	}
}

func TestRoutingTableInsertUpdatesExistingPeer(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(2))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	peer := testPeer(testID(0x80))
	rt.Insert(peer)

	updated := testPeer(peer.ID)
	updated.SetAgent("updated")

	result := rt.Insert(updated)

	if result.Status != InsertUpdated {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, InsertUpdated)
	}

	closest := rt.FindClosest(peer.ID, 1)
	if len(closest) != 1 {
		t.Fatalf("expected 1 closest peer")
	}

	if closest[0].Agent != "updated" {
		t.Fatalf("expected updated peer data")
	}
}

func TestRoutingTableInsertFullBucketNeedsPingOldest(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(2))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x90))
	p3 := testPeer(testID(0xa0))

	rt.Insert(p1)
	rt.Insert(p2)

	result := rt.Insert(p3)

	if result.Status != InsertNeedPingOldest {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, InsertNeedPingOldest)
	}

	if result.Oldest == nil || result.Oldest.ID != p1.ID {
		t.Fatalf("unexpected oldest peer")
	}

	if rt.HasPeer(p3.ID) {
		t.Fatalf("new peer must not be inserted before PING decision")
	}
}

func TestRoutingTableAddPeerKeepsOldPeerWhenPingAlive(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(2))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x90))
	p3 := testPeer(testID(0xa0))

	rt.Insert(p1)
	rt.Insert(p2)

	result := rt.AddPeer(p3, func(peer *model.Peer) bool {
		return true
	})

	if result.Status != InsertRejected {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, InsertRejected)
	}

	if !rt.HasPeer(p1.ID) {
		t.Fatalf("old live peer must remain")
	}

	if rt.HasPeer(p3.ID) {
		t.Fatalf("new peer must be rejected when oldest is alive")
	}
}

func TestRoutingTableAddPeerReplacesOldPeerWhenPingDead(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(2))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x90))
	p3 := testPeer(testID(0xa0))

	rt.Insert(p1)
	rt.Insert(p2)

	result := rt.AddPeer(p3, func(peer *model.Peer) bool {
		return false
	})

	if result.Status != InsertReplaced {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, InsertReplaced)
	}

	if rt.HasPeer(p1.ID) {
		t.Fatalf("dead oldest peer must be removed")
	}

	if !rt.HasPeer(p3.ID) {
		t.Fatalf("new peer must be inserted")
	}
}

func TestRoutingTableFindClosest(t *testing.T) {
	selfID := model.NodeID{}
	target := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(8))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testIDLast(0x02))
	p3 := testPeer(testIDLast(0x01))

	rt.Insert(p1)
	rt.Insert(p2)
	rt.Insert(p3)

	closest := rt.FindClosest(target, 3)

	requirePeerIDs(t, closest, []model.NodeID{
		p3.ID,
		p2.ID,
		p1.ID,
	})
}

func TestRoutingTableRemoveAndTouch(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(3))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x90))
	p3 := testPeer(testID(0xa0))

	rt.Insert(p1)
	rt.Insert(p2)
	rt.Insert(p3)

	if ok := rt.Touch(p1.ID); !ok {
		t.Fatal("expected Touch to succeed")
	}

	bucketPeers, err := rt.BucketPeers(0)
	if err != nil {
		t.Fatalf("BucketPeers() error: %v", err)
	}

	requirePeerIDs(t, bucketPeers, []model.NodeID{
		p2.ID,
		p3.ID,
		p1.ID,
	})

	if ok := rt.Remove(p1.ID); !ok {
		t.Fatal("expected Remove to succeed")
	}

	if rt.HasPeer(p1.ID) {
		t.Fatalf("peer must be removed")
	}
}

func TestRoutingTableRandomIDInBucket(t *testing.T) {
	selfID := model.NodeID{}

	rt, err := NewRoutingTableWithConfig(selfID, testRoutingConfig(3))
	if err != nil {
		t.Fatalf("NewRoutingTableWithConfig() error: %v", err)
	}

	id, err := rt.RandomIDInBucket(10)
	if err != nil {
		t.Fatalf("RandomIDInBucket() error: %v", err)
	}

	if got := rt.BucketIndex(id); got != 10 {
		t.Fatalf("generated ID in wrong bucket: got %d, want 10", got)
	}
}

func testRoutingConfig(k int) Config {
	cfg := DefaultConfig()
	cfg.K = k
	cfg.Alpha = 1
	cfg.EnforceTrustedPeers = false
	return cfg
}
