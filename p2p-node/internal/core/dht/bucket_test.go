// p2p-node/internal/core/dht/bucket_test.go
package dht

import (
	"testing"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

func TestKBucketAddOrUpdateAddsPeer(t *testing.T) {
	bucket := newKBucket(2)

	p1 := testPeer(testID(0x80))

	result := bucket.AddOrUpdate(p1)

	if result.Status != BucketInsertAdded {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, BucketInsertAdded)
	}

	if bucket.Len() != 1 {
		t.Fatalf("unexpected bucket len: got %d, want 1", bucket.Len())
	}

	if !bucket.Contains(p1.ID) {
		t.Fatalf("bucket must contain added peer")
	}
}

func TestKBucketAddOrUpdateUpdatesExistingPeerAndMovesToBack(t *testing.T) {
	bucket := newKBucket(3)

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x40))

	bucket.AddOrUpdate(p1)
	bucket.AddOrUpdate(p2)

	updatedP1 := testPeer(p1.ID)
	updatedP1.SetAgent("updated-agent")

	result := bucket.AddOrUpdate(updatedP1)
	if result.Status != BucketInsertUpdated {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, BucketInsertUpdated)
	}

	peers := bucket.Peers()

	requirePeerIDs(t, peers, []model.NodeID{
		p2.ID,
		p1.ID,
	})

	if peers[1].Agent != "updated-agent" {
		t.Fatalf("expected updated peer data to be stored")
	}
}

func TestKBucketFullReturnsOldestWithoutMutation(t *testing.T) {
	bucket := newKBucket(2)

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x40))
	p3 := testPeer(testID(0x20))

	bucket.AddOrUpdate(p1)
	bucket.AddOrUpdate(p2)

	result := bucket.AddOrUpdate(p3)

	if result.Status != BucketInsertFull {
		t.Fatalf("unexpected status: got %s, want %s", result.Status, BucketInsertFull)
	}

	if result.Oldest == nil {
		t.Fatal("expected oldest peer")
	}

	if result.Oldest.ID != p1.ID {
		t.Fatalf("unexpected oldest peer: got %x, want %x", result.Oldest.ID, p1.ID)
	}

	if bucket.Len() != 2 {
		t.Fatalf("full insert must not change len: got %d, want 2", bucket.Len())
	}

	if bucket.Contains(p3.ID) {
		t.Fatalf("full insert must not add new peer before PING decision")
	}
}

func TestKBucketReplace(t *testing.T) {
	bucket := newKBucket(2)

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x40))
	p3 := testPeer(testID(0x20))

	bucket.AddOrUpdate(p1)
	bucket.AddOrUpdate(p2)

	ok := bucket.Replace(p1.ID, p3)
	if !ok {
		t.Fatal("expected Replace to succeed")
	}

	if bucket.Contains(p1.ID) {
		t.Fatalf("old peer must be removed")
	}

	if !bucket.Contains(p3.ID) {
		t.Fatalf("new peer must be added")
	}

	requirePeerIDs(t, bucket.Peers(), []model.NodeID{
		p2.ID,
		p3.ID,
	})
}

func TestKBucketTouchMovesPeerToBack(t *testing.T) {
	bucket := newKBucket(3)

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testID(0x40))
	p3 := testPeer(testID(0x20))

	bucket.AddOrUpdate(p1)
	bucket.AddOrUpdate(p2)
	bucket.AddOrUpdate(p3)

	if ok := bucket.Touch(p1.ID); !ok {
		t.Fatal("expected Touch to succeed")
	}

	requirePeerIDs(t, bucket.Peers(), []model.NodeID{
		p2.ID,
		p3.ID,
		p1.ID,
	})
}

func TestKBucketRemove(t *testing.T) {
	bucket := newKBucket(2)

	p1 := testPeer(testID(0x80))

	bucket.AddOrUpdate(p1)

	if ok := bucket.Remove(p1.ID); !ok {
		t.Fatal("expected Remove to return true")
	}

	if bucket.Len() != 0 {
		t.Fatalf("unexpected bucket len: got %d, want 0", bucket.Len())
	}

	if bucket.Contains(p1.ID) {
		t.Fatalf("peer must be removed")
	}
}
