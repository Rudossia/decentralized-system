package dht

import (
	"github.com/Rudossia/p2p-node/internal/core/model"
)

const (
	K            = 20
	idLengthBits = 256
)

// RoutingTable управляет k-корзинами для хранения информации о других узлах.
// Массив корзин статичен, а потокобезопасность внутри самой структуры kBucket.
type RoutingTable struct {
	localID model.NodeID
	buckets []*kBucket // Исправлено: теперь это срез
}

// NewRoutingTable создает новую таблицу маршрутизации.
func NewRoutingTable(localID model.NodeID) *RoutingTable {
	rt := &RoutingTable{
		localID: localID,
		buckets: make([]*kBucket, model.NodeIDBytes*8),
	}
	for i := 0; i < model.NodeIDBytes*8; i++ {
		rt.buckets[i] = newKBucket()
	}
	return rt
}

// getBucketIndex вычисляет индекс корзины для данного NodeID.
func (rt *RoutingTable) getBucketIndex(id model.NodeID) int {
	prefixLen := rt.localID.CommonPrefixLen(id)
	if prefixLen >= model.NodeIDBytes*8 {
		return model.NodeIDBytes*8 - 1
	}
	return prefixLen
}

// AddPeer добавляет пир в таблицу.
// pingFunc - это callback (внедряется из инфраструктуры) для сетевой проверки старого пира.
func (rt *RoutingTable) AddPeer(p *model.Peer, pingFunc func(*model.Peer) bool) {
	bucketIndex := rt.getBucketIndex(p.ID)
	bucket := rt.buckets[bucketIndex]
	// 1. Если корзина не заполнена,  Add корзины сам безопасно добавит узел
	if bucket.Len() < K {
		bucket.Add(p)
		return
	}
	// 2. Если корзина полная, мы должны проверить, нет ли этого пира уже там чтобы просто обновить ему время LastSeen
	bucket.RLock()
	_, exists := bucket.m[p.ID]
	bucket.RUnlock()
	if exists {
		bucket.Add(p) // Add внутри себя переместит пир в конец
		return
	}
	// 3. Корзина полная и нового пира в ней нет. Получаем старейший узел.
	oldestPeer := bucket.GetOldest()
	if oldestPeer == nil {
		return
	}
	// 4. Сетевая проверка PING выполняется АСИНХРОННО!
	// Таблица маршрутизации не блокируется.
	go func(old, newP *model.Peer, b *kBucket) {
		// Если функция пинга не передана или узел мертв
		if pingFunc != nil && !pingFunc(old) {
			b.Remove(old.ID)
			b.Add(newP)
		}
		// Если узел жив, по правилам Kademlia новый узел отбрасывается
	}(oldestPeer, p, bucket)
}
