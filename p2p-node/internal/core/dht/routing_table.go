// p2p-node/internal/core/dht/routing_table.go
package dht

import (
	"fmt"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// PingFunc — callback для проверки живости peer.
//
// RoutingTable не должна знать про TCP/UDP/Protobuf.
// Поэтому реальный PING будет внедряться снаружи через usecase/infrastructure.
type PingFunc func(peer *model.Peer) bool

// InsertStatus описывает результат вставки peer в routing table.
type InsertStatus int

const (
	InsertInvalid InsertStatus = iota
	InsertIgnoredSelf
	InsertAdded
	InsertUpdated
	InsertNeedPingOldest
	InsertReplaced
	InsertRejected
)

func (s InsertStatus) String() string {
	switch s {
	case InsertIgnoredSelf:
		return "ignored_self"
	case InsertAdded:
		return "added"
	case InsertUpdated:
		return "updated"
	case InsertNeedPingOldest:
		return "need_ping_oldest"
	case InsertReplaced:
		return "replaced"
	case InsertRejected:
		return "rejected"
	default:
		return "invalid"
	}
}

// InsertResult — результат операции добавления peer в routing table.
type InsertResult struct {
	Status      InsertStatus
	BucketIndex int
	Peer        *model.Peer
	Oldest      *model.Peer
	Err         error
}

// NeedsPing показывает, что bucket полон и нужно проверить Oldest через PING.
func (r InsertResult) NeedsPing() bool {
	return r.Status == InsertNeedPingOldest && r.Oldest != nil
}

// RoutingTable — таблица маршрутизации Kademlia.
//
// Содержит 256 k-bucket для 256-битного NodeID.
// Bucket выбирается через CommonPrefixLen(localID, peerID).
//
// Важно:
//   - RoutingTable не выполняет сетевой ввод-вывод;
//   - RoutingTable не знает о TCP/UDP/Protobuf;
//   - RoutingTable только принимает решения: добавить, обновить,
//     запросить PING oldest, заменить dead peer.
type RoutingTable struct {
	localID model.NodeID
	cfg     Config
	buckets []*kBucket
}

// NewRoutingTable создаёт routing table с DefaultConfig.
//
// Сигнатура сохранена совместимой с уже начатым кодом:
//
//	NewRoutingTable(localID)
func NewRoutingTable(localID model.NodeID) *RoutingTable {
	rt, err := NewRoutingTableWithConfig(localID, DefaultConfig())
	if err != nil {
		panic(err)
	}

	return rt
}

// NewRoutingTableWithConfig создаёт routing table с явным Config.
func NewRoutingTableWithConfig(localID model.NodeID, cfg Config) (*RoutingTable, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	rt := &RoutingTable{
		localID: localID,
		cfg:     cfg,
		buckets: make([]*kBucket, IDLengthBits),
	}

	for i := 0; i < IDLengthBits; i++ {
		rt.buckets[i] = newKBucket(cfg.K)
	}

	return rt, nil
}

// LocalID возвращает NodeID локального узла.
func (rt *RoutingTable) LocalID() model.NodeID {
	return rt.localID
}

// Config возвращает копию конфигурации routing table.
func (rt *RoutingTable) Config() Config {
	return rt.cfg
}

// BucketCount возвращает число bucket.
func (rt *RoutingTable) BucketCount() int {
	return len(rt.buckets)
}

// BucketIndex возвращает индекс bucket для peerID.
func (rt *RoutingTable) BucketIndex(peerID model.NodeID) int {
	return BucketIndex(rt.localID, peerID)
}

// Insert добавляет peer без выполнения сетевого PING.
//
// Если bucket полон, метод возвращает InsertNeedPingOldest.
// После этого usecase-слой должен:
//   - проверить Oldest через PING;
//   - если Oldest жив — отбросить нового peer;
//   - если Oldest мёртв — вызвать ReplacePeer.
func (rt *RoutingTable) Insert(peer *model.Peer) InsertResult {
	if peer == nil {
		return InsertResult{
			Status:      InsertInvalid,
			BucketIndex: -1,
			Err:         ErrNilPeer,
		}
	}

	if peer.ID == rt.localID {
		return InsertResult{
			Status:      InsertIgnoredSelf,
			BucketIndex: -1,
			Peer:        peer.Clone(),
			Err:         ErrSelfPeer,
		}
	}

	if rt.cfg.EnforceTrustedPeers && !peer.Trusted {
		return InsertResult{
			Status:      InsertRejected,
			BucketIndex: -1,
			Peer:        peer.Clone(),
			Err:         ErrPeerRejected,
		}
	}

	bucketIndex, err := MustBucketIndex(rt.localID, peer.ID)
	if err != nil {
		return InsertResult{
			Status:      InsertInvalid,
			BucketIndex: -1,
			Peer:        peer.Clone(),
			Err:         err,
		}
	}

	bucket := rt.buckets[bucketIndex]
	result := bucket.AddOrUpdate(peer)

	switch result.Status {
	case BucketInsertAdded:
		return InsertResult{
			Status:      InsertAdded,
			BucketIndex: bucketIndex,
			Peer:        result.Peer,
		}

	case BucketInsertUpdated:
		return InsertResult{
			Status:      InsertUpdated,
			BucketIndex: bucketIndex,
			Peer:        result.Peer,
		}

	case BucketInsertFull:
		return InsertResult{
			Status:      InsertNeedPingOldest,
			BucketIndex: bucketIndex,
			Peer:        result.Peer,
			Oldest:      result.Oldest,
		}

	default:
		return InsertResult{
			Status:      InsertInvalid,
			BucketIndex: bucketIndex,
			Peer:        peer.Clone(),
			Err:         result.Err,
		}
	}
}

// AddPeer добавляет peer и, если bucket полон, может синхронно выполнить pingFunc.
//
// Метод оставлен для совместимости с уже начатым кодом, где AddPeer принимал
// callback pingFunc.
//
// Для будущего usecase-слоя лучше использовать пару:
//   - Insert(peer)
//   - ReplacePeer(oldID, newPeer)
//
// Так iterative lookup и routing maintenance будут более управляемыми и тестируемыми.
func (rt *RoutingTable) AddPeer(peer *model.Peer, pingFunc PingFunc) InsertResult {
	result := rt.Insert(peer)

	if !result.NeedsPing() {
		return result
	}

	// Если pingFunc не передан, routing table только сообщает,
	// что нужно проверить oldest peer.
	if pingFunc == nil {
		return result
	}

	oldestAlive := pingFunc(result.Oldest)

	if oldestAlive {
		// По правилам Kademlia:
		// если старый peer жив, он остаётся в bucket,
		// а новый peer отбрасывается.
		//
		// Дополнительно touch-им старый peer, потому что он только что ответил.
		rt.Touch(result.Oldest.ID)

		result.Status = InsertRejected
		result.Err = ErrPeerRejected
		return result
	}

	if rt.ReplacePeer(result.Oldest.ID, peer) {
		result.Status = InsertReplaced
		result.Err = nil
		return result
	}

	result.Status = InsertInvalid
	result.Err = fmt.Errorf("dht: failed to replace oldest peer")
	return result
}

// ReplacePeer удаляет oldID и добавляет newPeer в соответствующий bucket.
//
// Обычно вызывается после failed PING для oldest peer.
func (rt *RoutingTable) ReplacePeer(oldID model.NodeID, newPeer *model.Peer) bool {
	if newPeer == nil {
		return false
	}

	bucketIndex := rt.BucketIndex(newPeer.ID)
	if bucketIndex < 0 || bucketIndex >= len(rt.buckets) {
		return false
	}

	return rt.buckets[bucketIndex].Replace(oldID, newPeer)
}

// Remove удаляет peer из routing table.
func (rt *RoutingTable) Remove(id model.NodeID) bool {
	bucketIndex := rt.BucketIndex(id)
	if bucketIndex < 0 || bucketIndex >= len(rt.buckets) {
		return false
	}

	return rt.buckets[bucketIndex].Remove(id)
}

// Touch помечает peer как недавно виденный и перемещает его в конец bucket.
func (rt *RoutingTable) Touch(id model.NodeID) bool {
	bucketIndex := rt.BucketIndex(id)
	if bucketIndex < 0 || bucketIndex >= len(rt.buckets) {
		return false
	}

	return rt.buckets[bucketIndex].Touch(id)
}

// HasPeer проверяет, есть ли peer в routing table.
func (rt *RoutingTable) HasPeer(id model.NodeID) bool {
	bucketIndex := rt.BucketIndex(id)
	if bucketIndex < 0 || bucketIndex >= len(rt.buckets) {
		return false
	}

	return rt.buckets[bucketIndex].Contains(id)
}

// FindClosest возвращает до limit ближайших peer к target.
//
// Это ключевая операция для:
//   - FIND_NODE response;
//   - iterative lookup shortlist;
//   - выбора k ближайших peer для STORE;
//   - тестирования логарифмичности маршрутизации.
func (rt *RoutingTable) FindClosest(target model.NodeID, limit int) []*model.Peer {
	if limit <= 0 {
		return nil
	}

	return ClosestPeers(rt.AllPeers(), target, limit)
}

// GetClosest сохранён как alias для возможной совместимости с предыдущими вариантами.
func (rt *RoutingTable) GetClosest(target model.NodeID, limit int) []*model.Peer {
	return rt.FindClosest(target, limit)
}

// AllPeers возвращает всех peer из routing table.
func (rt *RoutingTable) AllPeers() []*model.Peer {
	peers := make([]*model.Peer, 0)

	for _, bucket := range rt.buckets {
		peers = append(peers, bucket.Peers()...)
	}

	return peers
}

// Len возвращает общее количество peer во всей routing table.
func (rt *RoutingTable) Len() int {
	total := 0

	for _, bucket := range rt.buckets {
		total += bucket.Len()
	}

	return total
}

// BucketLen возвращает число peer в конкретном bucket.
func (rt *RoutingTable) BucketLen(bucketIndex int) (int, error) {
	if bucketIndex < 0 || bucketIndex >= len(rt.buckets) {
		return 0, fmt.Errorf("%w: %d", ErrInvalidBucketIndex, bucketIndex)
	}

	return rt.buckets[bucketIndex].Len(), nil
}

// BucketPeers возвращает peer из конкретного bucket.
func (rt *RoutingTable) BucketPeers(bucketIndex int) ([]*model.Peer, error) {
	if bucketIndex < 0 || bucketIndex >= len(rt.buckets) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidBucketIndex, bucketIndex)
	}

	return rt.buckets[bucketIndex].Peers(), nil
}

// RandomIDInBucket генерирует случайный target ID для refresh указанного bucket.
//
// Refresh(bucket) в Kademlia обычно делает lookup по случайному ID,
// который попадает в диапазон данного bucket.
func (rt *RoutingTable) RandomIDInBucket(bucketIndex int) (model.NodeID, error) {
	return RandomIDInBucket(rt.localID, bucketIndex)
}

// RefreshTargetForBucket — более говорящее имя для RandomIDInBucket.
//
// Можно использовать в maintenance/usecase слое.
func (rt *RoutingTable) RefreshTargetForBucket(bucketIndex int) (model.NodeID, error) {
	return rt.RandomIDInBucket(bucketIndex)
}
