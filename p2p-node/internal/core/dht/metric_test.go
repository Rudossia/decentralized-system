// p2p-node/internal/core/dht/metric_test.go
package dht

import (
	"bytes"
	"testing"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

func TestDistanceComputesXOR(t *testing.T) {
	a := testIDWithBytes(0x0f, 0xaa)
	b := testIDWithBytes(0xf0, 0x55)

	got := Distance(a, b)

	if got[0] != 0xff {
		t.Fatalf("unexpected first byte XOR: got %02x, want ff", got[0])
	}

	if got[model.NodeIDBytes-1] != 0xff {
		t.Fatalf("unexpected last byte XOR: got %02x, want ff", got[model.NodeIDBytes-1])
	}
}

func TestCompareNodeID(t *testing.T) {
	a := testID(0x01)
	b := testID(0x02)

	if CompareNodeID(a, b) >= 0 {
		t.Fatalf("expected a < b")
	}

	if CompareNodeID(b, a) <= 0 {
		t.Fatalf("expected b > a")
	}

	if CompareNodeID(a, a) != 0 {
		t.Fatalf("expected a == a")
	}
}

func TestCompareDistance(t *testing.T) {
	target := model.NodeID{}

	near := testIDLast(0x01)
	far := testID(0x80)

	if CompareDistance(target, near, far) >= 0 {
		t.Fatalf("expected near to be closer than far")
	}

	if !LessByDistance(target, near, far) {
		t.Fatalf("LessByDistance returned false for closer peer")
	}
}

func TestBucketIndex(t *testing.T) {
	self := model.NodeID{}

	tests := []struct {
		name string
		peer model.NodeID
		want int
	}{
		{
			name: "first bit differs",
			peer: testID(0x80),
			want: 0,
		},
		{
			name: "second bit differs",
			peer: testID(0x40),
			want: 1,
		},
		{
			name: "last bit differs",
			peer: testIDLast(0x01),
			want: IDLengthBits - 1,
		},
		{
			name: "self",
			peer: self,
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BucketIndex(self, tt.peer)
			if got != tt.want {
				t.Fatalf("BucketIndex() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSortPeersByDistance(t *testing.T) {
	target := model.NodeID{}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testIDLast(0x02))
	p3 := testPeer(testIDLast(0x01))

	peers := []*model.Peer{p1, p2, p3}

	SortPeersByDistance(peers, target)

	requirePeerIDs(t, peers, []model.NodeID{
		p3.ID,
		p2.ID,
		p1.ID,
	})
}

func TestClosestPeersReturnsClonesAndLimit(t *testing.T) {
	target := model.NodeID{}

	p1 := testPeer(testID(0x80))
	p2 := testPeer(testIDLast(0x02))
	p3 := testPeer(testIDLast(0x01))

	closest := ClosestPeers([]*model.Peer{p1, p2, p3}, target, 2)

	requirePeerIDs(t, closest, []model.NodeID{
		p3.ID,
		p2.ID,
	})

	closest[0].SetAddresses([]string{"/ip4/10.0.0.1/tcp/9999"})

	if p3.HasAddress("/ip4/10.0.0.1/tcp/9999") {
		t.Fatalf("ClosestPeers returned original peer pointer, expected clone")
	}
}

func TestRandomIDInBucket(t *testing.T) {
	self := model.NodeID{}

	for _, bucketIndex := range []int{0, 1, 8, 128, IDLengthBits - 1} {
		t.Run("bucket", func(t *testing.T) {
			randomBytes := bytes.Repeat([]byte{0xff}, model.NodeIDBytes)

			id, err := RandomIDInBucketFromReader(self, bucketIndex, bytes.NewReader(randomBytes))
			if err != nil {
				t.Fatalf("RandomIDInBucketFromReader() error: %v", err)
			}

			gotBucket := BucketIndex(self, id)
			if gotBucket != bucketIndex {
				t.Fatalf("generated ID in wrong bucket: got %d, want %d", gotBucket, bucketIndex)
			}
		})
	}
}

func TestRandomIDInBucketRejectsBadIndex(t *testing.T) {
	self := model.NodeID{}

	_, err := RandomIDInBucket(self, -1)
	if err == nil {
		t.Fatal("expected error for negative bucket index")
	}

	_, err = RandomIDInBucket(self, IDLengthBits)
	if err == nil {
		t.Fatal("expected error for bucket index out of range")
	}
}
