package dht

import (
	"container/list"
	"sync"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// kBucket -  структура для хранения контактов (пиров) в Kademlia.
type kBucket struct {
	sync.RWMutex
	ll *list.List                     // Двусвязный список для эффективного перемещения элементов.
	m  map[model.NodeID]*list.Element // Карта для быстрого доступа к элементам списка.
}

func newKBucket() *kBucket {
	return &kBucket{
		ll: list.New(),
		m:  make(map[model.NodeID]*list.Element),
	}
}

// Add добавляет пир в корзину. Если пир уже существует, он обновляется (перемещается в конец).
func (b *kBucket) Add(p *model.Peer) {
	b.Lock()
	defer b.Unlock()

	if elem, ok := b.m[p.ID]; ok {
		// Пир уже существует, перемещаем в конец (самый новый).
		b.ll.MoveToBack(elem)
		elem.Value.(*model.Peer).Seen()
	} else {
		// Новый пир, добавляем в конец.
		elem := b.ll.PushBack(p)
		b.m[p.ID] = elem
	}
}

// GetOldest возвращает самый "старый" пир "из верха списка" без его удаления.
func (b *kBucket) GetOldest() *model.Peer {
	b.RLock()
	defer b.RUnlock()

	if elem := b.ll.Front(); elem != nil {
		return elem.Value.(*model.Peer)
	}
	return nil
}

// Remove удаляет пир из корзины.
func (b *kBucket) Remove(id model.NodeID) {
	b.Lock()
	defer b.Unlock()

	if elem, ok := b.m[id]; ok {
		b.ll.Remove(elem)
		delete(b.m, id)
	}
}

// Len возвращает количество пиров в корзине.
func (b *kBucket) Len() int {
	b.RLock()
	defer b.RUnlock()
	return b.ll.Len()
}

// Peers возвращает срез всех пиров в корзине.
func (b *kBucket) Peers() []*model.Peer {
	b.RLock()
	defer b.RUnlock()

	peers := make([]*model.Peer, 0, b.ll.Len())
	for elem := b.ll.Front(); elem != nil; elem = elem.Next() {
		peers = append(peers, elem.Value.(*model.Peer))
	}
	return peers
}
