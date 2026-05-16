// p2p-node/internal/core/dht/bucket.go
package dht

import (
	"container/list"
	"sync"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// BucketInsertStatus описывает результат попытки вставки peer в k-bucket.
type BucketInsertStatus int

const (
	BucketInsertInvalid BucketInsertStatus = iota
	BucketInsertAdded
	BucketInsertUpdated
	BucketInsertFull
)

func (s BucketInsertStatus) String() string {
	switch s {
	case BucketInsertAdded:
		return "added"
	case BucketInsertUpdated:
		return "updated"
	case BucketInsertFull:
		return "full"
	default:
		return "invalid"
	}
}

// BucketInsertResult — результат вставки peer в один k-bucket.
//
// Если Status == BucketInsertFull, Oldest содержит кандидата,
// которого routing table должна проверить через PING.
type BucketInsertResult struct {
	Status BucketInsertStatus
	Peer   *model.Peer
	Oldest *model.Peer
	Err    error
}

// kBucket хранит peer в порядке Kademlia LRU:
//
//   - Front() списка — самый старый peer;
//   - Back() списка — самый недавно виденный peer.
//
// При получении сообщения от существующего peer он перемещается в конец.
// Если bucket полон, новый peer не добавляется немедленно: routing table должна
// инициировать PING самого старого peer.
type kBucket struct {
	mu       sync.RWMutex
	capacity int
	ll       *list.List
	index    map[model.NodeID]*list.Element
}

func newKBucket(capacity int) *kBucket {
	if capacity <= 0 {
		capacity = DefaultBucketSize
	}

	return &kBucket{
		capacity: capacity,
		ll:       list.New(),
		index:    make(map[model.NodeID]*list.Element),
	}
}

// Add сохранён как короткий alias для уже начатого кода.
// В новой логике лучше использовать AddOrUpdate.
func (b *kBucket) Add(peer *model.Peer) BucketInsertResult {
	return b.AddOrUpdate(peer)
}

// AddOrUpdate добавляет peer или обновляет существующий.
//
// Правила:
//   - nil peer -> invalid;
//   - если peer уже есть, заменяем данные, обновляем LastSeen,
//     перемещаем в конец списка;
//   - если bucket не полон, добавляем в конец;
//   - если bucket полон, ничего не меняем и возвращаем oldest peer.
func (b *kBucket) AddOrUpdate(peer *model.Peer) BucketInsertResult {
	if peer == nil {
		return BucketInsertResult{
			Status: BucketInsertInvalid,
			Err:    ErrNilPeer,
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if elem, ok := b.index[peer.ID]; ok {
		oldPeer := elem.Value.(*model.Peer)
		merged := mergePeerForBucket(oldPeer, peer)

		merged.Seen()
		elem.Value = merged
		b.ll.MoveToBack(elem)

		return BucketInsertResult{
			Status: BucketInsertUpdated,
			Peer:   merged.Clone(),
		}
	}

	if b.ll.Len() >= b.capacity {
		oldest := b.oldestLocked()
		return BucketInsertResult{
			Status: BucketInsertFull,
			Peer:   peer.Clone(),
			Oldest: oldest,
		}
	}

	stored := peer.Clone()
	stored.Seen()

	elem := b.ll.PushBack(stored)
	b.index[stored.ID] = elem

	return BucketInsertResult{
		Status: BucketInsertAdded,
		Peer:   stored.Clone(),
	}
}

// Remove удаляет peer из bucket.
func (b *kBucket) Remove(id model.NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	elem, ok := b.index[id]
	if !ok {
		return false
	}

	b.ll.Remove(elem)
	delete(b.index, id)
	return true
}

// Contains проверяет, есть ли peer в bucket.
func (b *kBucket) Contains(id model.NodeID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, ok := b.index[id]
	return ok
}

// Oldest возвращает самый старый peer без удаления.
func (b *kBucket) Oldest() (*model.Peer, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	oldest := b.oldestLocked()
	if oldest == nil {
		return nil, false
	}

	return oldest, true
}

// GetOldest сохранён для совместимости со старой версией bucket.go.
func (b *kBucket) GetOldest() *model.Peer {
	oldest, _ := b.Oldest()
	return oldest
}

// Replace удаляет oldID и добавляет newPeer.
//
// Используется после того, как routing table проверила самый старый peer
// через PING и выяснила, что он не отвечает.
func (b *kBucket) Replace(oldID model.NodeID, newPeer *model.Peer) bool {
	if newPeer == nil {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	elem, ok := b.index[oldID]
	if !ok {
		return false
	}

	b.ll.Remove(elem)
	delete(b.index, oldID)

	stored := newPeer.Clone()
	stored.Seen()

	newElem := b.ll.PushBack(stored)
	b.index[stored.ID] = newElem

	return true
}

// Touch перемещает существующий peer в конец списка и обновляет LastSeen.
func (b *kBucket) Touch(id model.NodeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	elem, ok := b.index[id]
	if !ok {
		return false
	}

	peer := elem.Value.(*model.Peer)
	peer.Seen()
	b.ll.MoveToBack(elem)
	return true
}

// Len возвращает число peer в bucket.
func (b *kBucket) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ll.Len()
}

// Capacity возвращает максимальную ёмкость bucket.
func (b *kBucket) Capacity() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.capacity
}

// Peers возвращает peer в порядке от самого старого к самому новому.
//
// Возвращаются clone-объекты, чтобы внешний код не мутировал bucket напрямую.
func (b *kBucket) Peers() []*model.Peer {
	b.mu.RLock()
	defer b.mu.RUnlock()

	peers := make([]*model.Peer, 0, b.ll.Len())
	for elem := b.ll.Front(); elem != nil; elem = elem.Next() {
		peer := elem.Value.(*model.Peer)
		peers = append(peers, peer.Clone())
	}

	return peers
}

// oldestLocked возвращает clone самого старого peer.
//
// Важно: вызывать только при удержанном lock.
func (b *kBucket) oldestLocked() *model.Peer {
	elem := b.ll.Front()
	if elem == nil {
		return nil
	}

	peer := elem.Value.(*model.Peer)
	return peer.Clone()
}

// mergePeerForBucket объединяет старый peer из bucket и новый peer,
// пришедший из сети/обработчика.
//
// Идея: при обновлении контакта не потерять важные локальные свойства,
// например Trusted или FirstSeen.
func mergePeerForBucket(oldPeer, newPeer *model.Peer) *model.Peer {
	if oldPeer == nil {
		return newPeer.Clone()
	}
	if newPeer == nil {
		return oldPeer.Clone()
	}
	merged := newPeer.Clone()
	// FirstSeen должен отражать первое появление peer в локальной таблице.
	if !oldPeer.FirstSeen.IsZero() {
		merged.FirstSeen = oldPeer.FirstSeen
	}
	// Если peer уже был помечен доверенным, не сбрасываем это случайно.
	if oldPeer.Trusted {
		merged.Trusted = true
	}
	// Если новый peer не принёс публичный ключ, сохраняем старый.
	if len(merged.PubKey) == 0 && len(oldPeer.PubKey) > 0 {
		merged.SetPublicKey(oldPeer.PubKey)
	}
	// Если новый peer не указал версию протокола, сохраняем старую.
	if merged.ProtocolVersion == 0 {
		merged.ProtocolVersion = oldPeer.ProtocolVersion
	}
	// Если новый peer не указал Agent, сохраняем старый.
	if merged.Agent == "" {
		merged.Agent = oldPeer.Agent
	}
	return merged
}
