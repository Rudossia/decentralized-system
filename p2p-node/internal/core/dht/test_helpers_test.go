// p2p-node/internal/core/dht/test_helpers_test.go
package dht

import (
	"fmt"
	"testing"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

func testID(firstByte byte) model.NodeID {
	var id model.NodeID
	id[0] = firstByte
	return id
}

func testIDLast(lastByte byte) model.NodeID {
	var id model.NodeID
	id[model.NodeIDBytes-1] = lastByte
	return id
}

func testIDWithBytes(firstByte byte, lastByte byte) model.NodeID {
	var id model.NodeID
	id[0] = firstByte
	id[model.NodeIDBytes-1] = lastByte
	return id
}

func testPeer(id model.NodeID) *model.Peer {
	return model.NewPeer(
		id,
		[]string{fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 10000+int(id[0])+int(id[model.NodeIDBytes-1]))},
		[]byte{0x01, 0x02, 0x03},
	)
}

func testTrustedPeer(id model.NodeID) *model.Peer {
	peer := testPeer(id)
	peer.MarkTrusted()
	return peer
}

func requirePeerIDs(t *testing.T, peers []*model.Peer, want []model.NodeID) {
	t.Helper()

	if len(peers) != len(want) {
		t.Fatalf("unexpected peers count: got %d, want %d", len(peers), len(want))
	}

	for i := range want {
		if peers[i] == nil {
			t.Fatalf("peer[%d] is nil", i)
		}

		if peers[i].ID != want[i] {
			t.Fatalf("peer[%d] ID mismatch: got %x, want %x", i, peers[i].ID, want[i])
		}
	}
}
