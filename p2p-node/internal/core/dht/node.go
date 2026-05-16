// p2p-node/internal/core/dht/node.go
package dht

import (
	"fmt"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// Node — локальное ядро DHT/Kademlia-узла.
//
//	Node из core/dht НЕ открывает TCP-сокеты, НЕ сериализует Protobuf,  НЕ выполняет криптографическую проверку сам.
//	только за доменную DHT-логику:
//	 - хранит Self peer;
//	 - хранит RoutingTable;
//	 - хранит локальный Store;
//	 - обрабатывает семантику PING/FIND_NODE/FIND_VALUE/STORE.
type Node struct {
	self  *model.Peer
	cfg   Config
	table *RoutingTable
	store *Store
}

// NewNode создаёт локальный DHT-узел.
func NewNode(self *model.Peer, cfg Config) (*Node, error) {
	if self == nil {
		return nil, ErrNilPeer
	}
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	table, err := NewRoutingTableWithConfig(self.ID, cfg)
	if err != nil {
		return nil, err
	}
	return &Node{self: self.Clone(), cfg: cfg, table: table, store: NewStore(cfg)}, nil
}

// NewNodeFromID создаёт DHT-узел только по NodeID.
// unit-тестов .
func NewNodeFromID(id model.NodeID, cfg Config) (*Node, error) {
	peer := model.NewPeer(id, nil, nil)
	return NewNode(peer, cfg)
}

// Self возвращает копию локального peer.
func (n *Node) Self() *model.Peer {
	if n == nil || n.self == nil {
		return nil
	}
	return n.self.Clone()
}

// Config возвращает конфигурацию узла.
func (n *Node) Config() Config {
	if n == nil {
		return DefaultConfig()
	}
	return n.cfg
}

// RoutingTable возвращает таблицу маршрутизации.
// Возвращается указатель намеренно: это доменный объект,
// через который usecase-слой будет выполнять Insert/FindClosest/RefreshTarget.
func (n *Node) RoutingTable() *RoutingTable {
	if n == nil {
		return nil
	}
	return n.table
}

// Store возвращает локальное DHT-хранилище.
func (n *Node) Store() *Store {
	if n == nil {
		return nil
	}
	return n.store
}

// InsertPeer добавляет peer в routing table.
func (n *Node) InsertPeer(peer *model.Peer) InsertResult {
	if n == nil || n.table == nil {
		return InsertResult{
			Status: InsertInvalid,
			Err:    ErrNilNode,
		}
	}
	return n.table.Insert(peer)
}

// AddPeer добавляет peer в routing table с возможной PING-проверкой oldest.
//
// Это compatibility wrapper поверх RoutingTable.AddPeer.
func (n *Node) AddPeer(peer *model.Peer, pingFunc PingFunc) InsertResult {
	if n == nil || n.table == nil {
		return InsertResult{
			Status: InsertInvalid,
			Err:    ErrNilNode,
		}
	}

	return n.table.AddPeer(peer, pingFunc)
}

// RemovePeer удаляет peer из routing table.
func (n *Node) RemovePeer(id model.NodeID) bool {
	if n == nil || n.table == nil {
		return false
	}

	return n.table.Remove(id)
}

// ClosestPeers возвращает ближайших известных peer к target.
func (n *Node) ClosestPeers(target model.NodeID, limit int) []*model.Peer {
	if n == nil || n.table == nil {
		return nil
	}

	if limit <= 0 {
		limit = n.cfg.K
	}

	return n.table.FindClosest(target, limit)
}

// HandlePing обрабатывает входящий PING.
//
// Поведение:
//   - обновить routing table информацией об отправителе;
//   - вернуть локальный peer как ответ.
func (n *Node) HandlePing(from *model.Peer) (*model.Peer, error) {
	if n == nil {
		return nil, ErrNilNode
	}

	if from != nil {
		n.InsertPeer(from)
	}

	return n.Self(), nil
}

// HandleFindNode обрабатывает входящий FIND_NODE.
//
// Поведение:
//   - обновить routing table по from;
//   - вернуть k ближайших известных peer к target.
func (n *Node) HandleFindNode(from *model.Peer, target model.NodeID, limit int) ([]*model.Peer, error) {
	if n == nil {
		return nil, ErrNilNode
	}

	if from != nil {
		n.InsertPeer(from)
	}

	if limit <= 0 {
		limit = n.cfg.K
	}

	return n.ClosestPeers(target, limit), nil
}

// HandleFindValue обрабатывает входящий FIND_VALUE.
// Логика Kademlia:
//   - если значение найдено локально — вернуть значение;
//   - если не найдено — вернуть ближайших известных peer.
func (n *Node) HandleFindValue(from *model.Peer, key model.NodeID, limit int) (FindValueResult, error) {
	if n == nil {
		return FindValueResult{}, ErrNilNode
	}
	if from != nil {
		n.InsertPeer(from)
	}
	if limit <= 0 {
		limit = n.cfg.K
	}
	// 1. Сначала пробуем найти PeerRecord.
	if record, ok := n.store.GetPeerRecord(key); ok {
		return FindValueResult{
			Key:        key,
			Found:      true,
			PeerRecord: &record,
			Closest:    nil,
		}, nil
	}

	// 2. Потом пробуем обычную Key -> Value запись.
	if record, ok := n.store.Get(key); ok {
		return FindValueResult{
			Key:     key,
			Found:   true,
			Value:   cloneBytes(record.Value),
			Closest: nil,
		}, nil
	}

	// 3. Если значения нет — возвращаем ближайших peer.
	return FindValueResult{
		Key:     key,
		Found:   false,
		Closest: n.ClosestPeers(key, limit),
	}, nil
}

// HandleStoreRecord обрабатывает входящий STORE для обычной Key -> Value записи.
func (n *Node) HandleStoreRecord(from *model.Peer, record Record) error {
	if n == nil {
		return ErrNilNode
	}

	if from != nil {
		n.InsertPeer(from)
	}

	if n.store == nil {
		return ErrNilStore
	}

	return n.store.Put(record)
}

// HandleStoreValue — helper для входящего STORE(Key, Value).
func (n *Node) HandleStoreValue(from *model.Peer, key model.NodeID, value []byte, publisherID model.NodeID, seq uint64) error {
	if n == nil {
		return ErrNilNode
	}

	if from != nil {
		n.InsertPeer(from)
	}

	if n.store == nil {
		return ErrNilStore
	}

	return n.store.PutValue(key, value, publisherID, seq)
}

// HandleStorePeerRecord обрабатывает входящий STORE для PeerRecord.
func (n *Node) HandleStorePeerRecord(from *model.Peer, record model.PeerRecord) error {
	if n == nil {
		return ErrNilNode
	}

	if from != nil {
		n.InsertPeer(from)
	}

	if n.store == nil {
		return ErrNilStore
	}

	return n.store.PutPeerRecord(record)
}

// PublishSelfRecord создаёт и сохраняет локальную PeerRecord для самого узла.
//
// В реальной сети после этого usecase/publish должен выполнить IterativeStore,
// чтобы разложить запись на k ближайших узлов.
func (n *Node) PublishSelfRecord(seq uint64) (model.PeerRecord, error) {
	if n == nil || n.self == nil {
		return model.PeerRecord{}, ErrNilNode
	}

	record := n.self.ToPeerRecord(seq, n.cfg.RecordTTL)

	if err := n.store.PutPeerRecord(record); err != nil {
		return model.PeerRecord{}, err
	}

	return record.Clone(), nil
}

// LocalPeerRecord возвращает локально сохранённую PeerRecord по NodeID.
func (n *Node) LocalPeerRecord(nodeID model.NodeID) (model.PeerRecord, bool) {
	if n == nil || n.store == nil {
		return model.PeerRecord{}, false
	}

	return n.store.GetPeerRecord(nodeID)
}

// LocalValue возвращает обычное локальное значение по key.
func (n *Node) LocalValue(key model.NodeID) (Record, bool) {
	if n == nil || n.store == nil {
		return Record{}, false
	}

	return n.store.Get(key)
}

// String возвращает диагностическое описание DHT-узла.
func (n *Node) String() string {
	if n == nil {
		return "dht.Node(nil)"
	}

	return fmt.Sprintf(
		"dht.Node{peers:%d, records:%d, peer_records:%d}",
		n.table.Len(),
		n.store.Len(),
		n.store.PeerRecordLen(),
	)
}
