// p2p-node/internal/core/dht/store_test.go
package dht

import (
	"testing"
	"time"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

func TestStorePutGetDeleteRecord(t *testing.T) {
	store := NewStore(DefaultConfig())

	key := testIDLast(0x01)
	publisher := testID(0x80)

	record := NewRecord(key, []byte("hello"), publisher, 1, time.Hour)

	if err := store.Put(record); err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	got, ok := store.Get(key)
	if !ok {
		t.Fatal("expected record to exist")
	}

	if string(got.Value) != "hello" {
		t.Fatalf("unexpected value: got %q", got.Value)
	}

	if !store.Delete(key) {
		t.Fatal("expected Delete to return true")
	}

	if _, ok := store.Get(key); ok {
		t.Fatal("expected record to be deleted")
	}
}

func TestStoreKeepsNewerRecordBySeq(t *testing.T) {
	store := NewStore(DefaultConfig())

	key := testIDLast(0x01)
	publisher := testID(0x80)

	newRecord := NewRecord(key, []byte("new"), publisher, 2, time.Hour)
	oldRecord := NewRecord(key, []byte("old"), publisher, 1, time.Hour)

	if err := store.Put(newRecord); err != nil {
		t.Fatalf("Put(newRecord) error: %v", err)
	}

	if err := store.Put(oldRecord); err != nil {
		t.Fatalf("Put(oldRecord) error: %v", err)
	}

	got, ok := store.Get(key)
	if !ok {
		t.Fatal("expected record")
	}

	if string(got.Value) != "new" {
		t.Fatalf("old record overwrote new record: got %q", got.Value)
	}
}

func TestStoreGetDeletesExpiredRecord(t *testing.T) {
	store := NewStore(DefaultConfig())

	key := testIDLast(0x01)
	publisher := testID(0x80)

	expired := NewRecord(key, []byte("expired"), publisher, 1, time.Hour)
	expired.ExpiresAt = time.Now().Add(-time.Hour)

	store.records[key] = expired

	if _, ok := store.Get(key); ok {
		t.Fatal("expected expired record to be hidden")
	}

	if _, exists := store.records[key]; exists {
		t.Fatal("expected expired record to be deleted")
	}
}

func TestStorePutGetPeerRecord(t *testing.T) {
	store := NewStore(DefaultConfig())

	nodeID := testID(0x80)

	record := model.NewPeerRecord(
		nodeID,
		[]string{"/ip4/127.0.0.1/tcp/9001"},
		[]byte{0x01},
		[]string{"dht", "tcp"},
		1,
		"p2p-node-test",
		1,
		time.Hour,
	)

	if err := store.PutPeerRecord(record); err != nil {
		t.Fatalf("PutPeerRecord() error: %v", err)
	}

	got, ok := store.GetPeerRecord(nodeID)
	if !ok {
		t.Fatal("expected peer record")
	}

	if got.NodeID != nodeID {
		t.Fatalf("unexpected NodeID")
	}

	if !got.HasSignature() {
		// Подпись пока не обязательна. Проверяем явно, что отсутствие подписи
		// не мешает хранению PeerRecord на этом уровне.
		t.Log("peer record has no signature, as expected for current core test")
	}
}

func TestStoreKeepsNewerPeerRecordBySeq(t *testing.T) {
	store := NewStore(DefaultConfig())

	nodeID := testID(0x80)

	newRecord := model.NewPeerRecord(
		nodeID,
		[]string{"/ip4/127.0.0.1/tcp/9002"},
		[]byte{0x01},
		[]string{"dht"},
		1,
		"new",
		2,
		time.Hour,
	)

	oldRecord := model.NewPeerRecord(
		nodeID,
		[]string{"/ip4/127.0.0.1/tcp/9001"},
		[]byte{0x01},
		[]string{"dht"},
		1,
		"old",
		1,
		time.Hour,
	)

	if err := store.PutPeerRecord(newRecord); err != nil {
		t.Fatalf("PutPeerRecord(newRecord) error: %v", err)
	}

	if err := store.PutPeerRecord(oldRecord); err != nil {
		t.Fatalf("PutPeerRecord(oldRecord) error: %v", err)
	}

	got, ok := store.GetPeerRecord(nodeID)
	if !ok {
		t.Fatal("expected peer record")
	}

	if got.Agent != "new" {
		t.Fatalf("old peer record overwrote new peer record: got agent %q", got.Agent)
	}
}

func TestStoreDeleteExpired(t *testing.T) {
	store := NewStore(DefaultConfig())

	now := time.Now().UTC()

	key := testIDLast(0x01)
	expiredRecord := NewRecord(key, []byte("expired"), testID(0x80), 1, time.Hour)
	expiredRecord.ExpiresAt = now.Add(-time.Hour)
	store.records[key] = expiredRecord

	nodeID := testID(0x40)
	expiredPeerRecord := model.NewPeerRecord(
		nodeID,
		[]string{"/ip4/127.0.0.1/tcp/9001"},
		nil,
		[]string{"dht"},
		1,
		"test",
		1,
		time.Hour,
	)
	expiredPeerRecord.ExpiresAt = now.Add(-time.Hour)
	store.peerRecords[nodeID] = expiredPeerRecord

	deletedRecords, deletedPeerRecords := store.DeleteExpired(now)

	if deletedRecords != 1 {
		t.Fatalf("unexpected deleted records: got %d, want 1", deletedRecords)
	}

	if deletedPeerRecords != 1 {
		t.Fatalf("unexpected deleted peer records: got %d, want 1", deletedPeerRecords)
	}
}

func TestStoreRecordsForReplicationAndMarkReplicated(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicateInterval = time.Hour

	store := NewStore(cfg)

	key := testIDLast(0x01)
	record := NewRecord(key, []byte("hello"), testID(0x80), 1, time.Hour)

	if err := store.Put(record); err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	now := time.Now().UTC()

	records := store.RecordsForReplication(now)
	if len(records) != 1 {
		t.Fatalf("expected 1 record for replication, got %d", len(records))
	}

	if !store.MarkReplicated(key, now) {
		t.Fatal("expected MarkReplicated to succeed")
	}

	records = store.RecordsForReplication(now.Add(10 * time.Minute))
	if len(records) != 0 {
		t.Fatalf("expected no records before replicate interval, got %d", len(records))
	}
}
