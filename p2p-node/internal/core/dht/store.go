// p2p-node/internal/core/dht/store.go
package dht

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

var (
	ErrNilNode        = errors.New("dht: nil node")
	ErrNilStore       = errors.New("dht: nil store")
	ErrRecordNotFound = errors.New("dht: record not found")
	ErrRecordExpired  = errors.New("dht: record expired")
	ErrInvalidRecord  = errors.New("dht: invalid record")
	ErrEmptyValue     = errors.New("dht: empty value")

	ErrLookupNoCandidates = errors.New("dht: lookup has no candidates")
)

// Record — локально хранимая DHT-запись общего вида.
//
// Это задел под обычный Kademlia STORE(Key, Value).
// Для публикации записей о пирах отдельно есть PeerRecord-хранилище ниже,
// потому что model.PeerRecord содержит доменные поля: NodeID, Addrs,
// PublicKey, Capabilities, Seq, Signature и т.д.
type Record struct {
	Key model.NodeID

	// Value — полезная нагрузка записи.
	//
	// На уровне core/dht это просто байты.
	// Сериализация конкретных структур должна происходить выше:
	// usecase/protocol/codec.
	Value []byte

	// PublisherID — узел, опубликовавший запись.
	PublisherID model.NodeID

	// Seq — версия записи.
	//
	// Используется для выбора более новой записи при повторной публикации.
	Seq uint64

	CreatedAt time.Time
	UpdatedAt time.Time
	ExpiresAt time.Time

	// LastReplicatedAt — когда эта запись последний раз реплицировалась
	// локальным узлом на k ближайших узлов.
	LastReplicatedAt time.Time
}

// NewRecord создаёт обычную DHT-запись Key -> Value.
func NewRecord(key model.NodeID, value []byte, publisherID model.NodeID, seq uint64, ttl time.Duration) Record {
	now := time.Now().UTC()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	return Record{
		Key:              key,
		Value:            cloneBytes(value),
		PublisherID:      publisherID,
		Seq:              seq,
		CreatedAt:        now,
		UpdatedAt:        now,
		ExpiresAt:        expiresAt,
		LastReplicatedAt: time.Time{},
	}
}

// Clone возвращает глубокую копию Record.
func (r Record) Clone() Record {
	return Record{
		Key:              r.Key,
		Value:            cloneBytes(r.Value),
		PublisherID:      r.PublisherID,
		Seq:              r.Seq,
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
		ExpiresAt:        r.ExpiresAt,
		LastReplicatedAt: r.LastReplicatedAt,
	}
}

// Validate проверяет базовую корректность записи.
func (r Record) Validate(now time.Time) error {
	if len(r.Value) == 0 {
		return ErrEmptyValue
	}

	if !now.IsZero() && r.IsExpired(now) {
		return ErrRecordExpired
	}

	return nil
}

// IsExpired проверяет, истёк ли срок жизни записи.
func (r Record) IsExpired(now time.Time) bool {
	if r.ExpiresAt.IsZero() {
		return false
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	return !now.UTC().Before(r.ExpiresAt.UTC())
}

// NewerThan возвращает true, если r новее other.
func (r Record) NewerThan(other Record) bool {
	if r.Seq != other.Seq {
		return r.Seq > other.Seq
	}

	return r.UpdatedAt.After(other.UpdatedAt)
}

// NeedsReplication проверяет, пора ли реплицировать запись.
func (r Record) NeedsReplication(now time.Time, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	if r.LastReplicatedAt.IsZero() {
		return true
	}

	return now.UTC().Sub(r.LastReplicatedAt.UTC()) >= interval
}

// Store — локальное хранилище DHT-записей.
//
// В нём есть два слоя:
//
//  1. records:
//     обычный Kademlia Key -> Value.
//
//  2. peerRecords:
//     доменные записи о пирах NodeID -> PeerRecord,
//     которые нужны для P2P discovery и публикации адресов узлов.
//
// Store потокобезопасен.
type Store struct {
	mu sync.RWMutex

	cfg Config

	records     map[model.NodeID]Record
	peerRecords map[model.NodeID]model.PeerRecord
}

// NewStore создаёт локальное DHT-хранилище.
func NewStore(cfg Config) *Store {
	cfg = cfg.Normalize()

	return &Store{
		cfg:         cfg,
		records:     make(map[model.NodeID]Record),
		peerRecords: make(map[model.NodeID]model.PeerRecord),
	}
}

// Put сохраняет обычную Key -> Value запись.
//
// Если запись с таким ключом уже есть, сохраняется более новая версия.
func (s *Store) Put(record Record) error {
	if s == nil {
		return ErrNilStore
	}

	now := time.Now().UTC()

	if err := record.Validate(now); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record = record.Clone()

	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now

	if record.ExpiresAt.IsZero() && s.cfg.RecordTTL > 0 {
		record.ExpiresAt = now.Add(s.cfg.RecordTTL)
	}

	old, exists := s.records[record.Key]
	if exists && !record.NewerThan(old) {
		return nil
	}

	s.records[record.Key] = record
	return nil
}

// PutValue — короткий helper для сохранения байтового значения.
func (s *Store) PutValue(key model.NodeID, value []byte, publisherID model.NodeID, seq uint64) error {
	record := NewRecord(key, value, publisherID, seq, s.cfg.RecordTTL)
	return s.Put(record)
}

// Get возвращает обычную DHT-запись по ключу.
func (s *Store) Get(key model.NodeID) (Record, bool) {
	if s == nil {
		return Record{}, false
	}

	now := time.Now().UTC()

	s.mu.RLock()
	record, ok := s.records[key]
	s.mu.RUnlock()

	if !ok {
		return Record{}, false
	}

	if record.IsExpired(now) {
		s.Delete(key)
		return Record{}, false
	}

	return record.Clone(), true
}

// Delete удаляет обычную DHT-запись.
func (s *Store) Delete(key model.NodeID) bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.records[key]; !ok {
		return false
	}

	delete(s.records, key)
	return true
}

// PutPeerRecord сохраняет публикуемую DHT-запись о пире.
func (s *Store) PutPeerRecord(record model.PeerRecord) error {
	if s == nil {
		return ErrNilStore
	}

	now := time.Now().UTC()

	if err := record.Validate(now); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record = record.Clone()

	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}

	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}

	if record.ExpiresAt.IsZero() && s.cfg.RecordTTL > 0 {
		record.ExpiresAt = now.Add(s.cfg.RecordTTL)
	}

	old, exists := s.peerRecords[record.NodeID]
	if exists && !record.NewerThan(old) {
		return nil
	}

	s.peerRecords[record.NodeID] = record
	return nil
}

// GetPeerRecord возвращает PeerRecord по NodeID.
func (s *Store) GetPeerRecord(nodeID model.NodeID) (model.PeerRecord, bool) {
	if s == nil {
		return model.PeerRecord{}, false
	}

	now := time.Now().UTC()

	s.mu.RLock()
	record, ok := s.peerRecords[nodeID]
	s.mu.RUnlock()

	if !ok {
		return model.PeerRecord{}, false
	}

	if record.IsExpired(now) {
		s.DeletePeerRecord(nodeID)
		return model.PeerRecord{}, false
	}

	return record.Clone(), true
}

// DeletePeerRecord удаляет PeerRecord по NodeID.
func (s *Store) DeletePeerRecord(nodeID model.NodeID) bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.peerRecords[nodeID]; !ok {
		return false
	}

	delete(s.peerRecords, nodeID)
	return true
}

// DeleteExpired удаляет все истёкшие записи.
//
// Возвращает:
//   - число удалённых обычных records;
//   - число удалённых peerRecords.
func (s *Store) DeleteExpired(now time.Time) (int, int) {
	if s == nil {
		return 0, 0
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	deletedRecords := 0
	for key, record := range s.records {
		if record.IsExpired(now) {
			delete(s.records, key)
			deletedRecords++
		}
	}

	deletedPeerRecords := 0
	for nodeID, record := range s.peerRecords {
		if record.IsExpired(now) {
			delete(s.peerRecords, nodeID)
			deletedPeerRecords++
		}
	}

	return deletedRecords, deletedPeerRecords
}

// RecordsForReplication возвращает обычные записи, которые пора реплицировать.
func (s *Store) RecordsForReplication(now time.Time) []Record {
	if s == nil {
		return nil
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]Record, 0)

	for _, record := range s.records {
		if record.IsExpired(now) {
			continue
		}
		if record.NeedsReplication(now, s.cfg.ReplicateInterval) {
			result = append(result, record.Clone())
		}
	}

	return result
}

// MarkReplicated отмечает, что обычная запись была реплицирована.
func (s *Store) MarkReplicated(key model.NodeID, at time.Time) bool {
	if s == nil {
		return false
	}

	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[key]
	if !ok {
		return false
	}

	record.LastReplicatedAt = at.UTC()
	s.records[key] = record
	return true
}

// PeerRecords возвращает все актуальные PeerRecord.
func (s *Store) PeerRecords(now time.Time) []model.PeerRecord {
	if s == nil {
		return nil
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]model.PeerRecord, 0, len(s.peerRecords))

	for _, record := range s.peerRecords {
		if record.IsExpired(now) {
			continue
		}
		result = append(result, record.Clone())
	}

	return result
}

// Len возвращает число обычных Key -> Value записей.
func (s *Store) Len() int {
	if s == nil {
		return 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.records)
}

// PeerRecordLen возвращает число PeerRecord-записей.
func (s *Store) PeerRecordLen() int {
	if s == nil {
		return 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.peerRecords)
}

// String возвращает краткое диагностическое описание Store.
func (s *Store) String() string {
	if s == nil {
		return "dht.Store(nil)"
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("dht.Store{records:%d, peer_records:%d}", len(s.records), len(s.peerRecords))
}
