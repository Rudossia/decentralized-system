// p2p-node/internal/core/dht/metric.go
package dht

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"sort"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// Distance возвращает XOR-расстояние между двумя NodeID.
//
// В Kademlia близость узлов определяется не географически и не по IP,
// а через XOR-метрику:
//
//	Distance(a, b) = a XOR b
//
// Чем меньше результат XOR как большое беззнаковое число,
// тем ближе идентификаторы в DHT-пространстве.
func Distance(a, b model.NodeID) model.NodeID {
	return a.XORDistance(b)
}

// CompareNodeID сравнивает два NodeID как большие беззнаковые числа.
//
// Возвращает:
//   - -1, если a < b;
//   - 0, если a == b;
//   - +1, если a > b.
func CompareNodeID(a, b model.NodeID) int {
	return bytes.Compare(a[:], b[:])
}

// IsZeroID проверяет, что NodeID равен нулевому значению.
func IsZeroID(id model.NodeID) bool {
	var zero model.NodeID
	return id == zero
}

// CompareDistance сравнивает расстояния от двух peer ID до target.
//
// Возвращает:
//   - -1, если a ближе к target, чем b;
//   - 0, если расстояния равны;
//   - +1, если a дальше от target, чем b.
func CompareDistance(target, a, b model.NodeID) int {
	distA := Distance(target, a)
	distB := Distance(target, b)
	return CompareNodeID(distA, distB)
}

// LessByDistance возвращает true, если a ближе к target, чем b.
func LessByDistance(target, a, b model.NodeID) bool {
	return CompareDistance(target, a, b) < 0
}

// BucketIndex вычисляет индекс k-bucket для peer относительно self.
//
// Используется модель bucket по длине общего префикса:
//
//	index = CommonPrefixLen(self, peer)
//
// Тогда:
//   - bucket 0 содержит самые дальние peer, отличающиеся уже в первом бите;
//   - bucket 255 содержит самые близкие peer, у которых совпадают первые 255 бит;
//   - если self == peer, возвращаем -1, потому что себя в routing table не добавляем.
//
// Это согласуется с текущим model.NodeID.CommonPrefixLen().
func BucketIndex(self, peer model.NodeID) int {
	if self == peer {
		return -1
	}

	prefixLen := self.CommonPrefixLen(peer)
	if prefixLen >= IDLengthBits {
		return -1
	}

	return prefixLen
}

// MustBucketIndex делает то же, что BucketIndex, но возвращает ошибку.
//
// Удобно для публичных методов routing table.
func MustBucketIndex(self, peer model.NodeID) (int, error) {
	index := BucketIndex(self, peer)
	if index < 0 || index >= IDLengthBits {
		return -1, fmt.Errorf("%w: self and peer have identical NodeID", ErrInvalidBucketIndex)
	}

	return index, nil
}

// ByDistance реализует sort.Interface для сортировки peer по XOR-расстоянию.
//
// Этот тип сохранён, чтобы не ломать уже начатую структуру пакета dht.
type ByDistance struct {
	Peers  []*model.Peer
	Target model.NodeID
}

func (b ByDistance) Len() int {
	return len(b.Peers)
}

func (b ByDistance) Swap(i, j int) {
	b.Peers[i], b.Peers[j] = b.Peers[j], b.Peers[i]
}

func (b ByDistance) Less(i, j int) bool {
	if b.Peers[i] == nil {
		return false
	}
	if b.Peers[j] == nil {
		return true
	}

	return LessByDistance(b.Target, b.Peers[i].ID, b.Peers[j].ID)
}

// SortPeersByDistance сортирует peer по близости к target.
//
// Сортировка выполняется in-place.
// nil-peer уводятся в конец.
func SortPeersByDistance(peers []*model.Peer, target model.NodeID) {
	sort.Sort(ByDistance{
		Peers:  peers,
		Target: target,
	})
}

// ClosestPeers возвращает до limit ближайших peer к target.
//
// Метод:
//   - не изменяет входной срез;
//   - исключает nil;
//   - возвращает clone каждого peer, чтобы внешний код случайно не мутировал
//     объекты из routing table.
func ClosestPeers(peers []*model.Peer, target model.NodeID, limit int) []*model.Peer {
	if limit <= 0 || len(peers) == 0 {
		return nil
	}

	filtered := make([]*model.Peer, 0, len(peers))
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		filtered = append(filtered, peer.Clone())
	}

	SortPeersByDistance(filtered, target)

	if len(filtered) > limit {
		filtered = filtered[:limit]
	}

	return filtered
}

// RandomIDInBucket генерирует случайный NodeID, который попадёт в заданный bucket
// относительно self.
//
// bucketIndex интерпретируется как CommonPrefixLen:
//
//	bucketIndex = 0   -> первый бит отличается;
//	bucketIndex = 255 -> первые 255 бит совпадают, отличается последний бит.
func RandomIDInBucket(self model.NodeID, bucketIndex int) (model.NodeID, error) {
	return RandomIDInBucketFromReader(self, bucketIndex, rand.Reader)
}

// RandomIDInBucketFromReader делает то же, что RandomIDInBucket,
// но источник случайности передаётся явно.
//
// Это нужно для unit-тестов, чтобы можно было передать детерминированный reader.
func RandomIDInBucketFromReader(self model.NodeID, bucketIndex int, reader io.Reader) (model.NodeID, error) {
	if bucketIndex < 0 || bucketIndex >= IDLengthBits {
		return model.NodeID{}, fmt.Errorf("%w: %d", ErrInvalidBucketIndex, bucketIndex)
	}
	if reader == nil {
		reader = rand.Reader
	}

	var id model.NodeID
	if _, err := io.ReadFull(reader, id[:]); err != nil {
		return model.NodeID{}, err
	}

	// Первые bucketIndex бит должны совпадать с self.
	for bitIndex := 0; bitIndex < bucketIndex; bitIndex++ {
		setBit(&id, bitIndex, getBit(self, bitIndex))
	}

	// Бит bucketIndex обязан отличаться.
	if getBit(self, bucketIndex) == 0 {
		setBit(&id, bucketIndex, 1)
	} else {
		setBit(&id, bucketIndex, 0)
	}

	// Остальные биты уже случайные.
	return id, nil
}

// getBit возвращает bitIndex-й бит NodeID, считая от старшего бита.
//
// bitIndex = 0   -> самый старший бит id[0]
// bitIndex = 255 -> самый младший бит id[31]
func getBit(id model.NodeID, bitIndex int) byte {
	byteIndex := bitIndex / 8
	offset := uint(7 - (bitIndex % 8))

	if (id[byteIndex] & (1 << offset)) == 0 {
		return 0
	}
	return 1
}

// setBit выставляет bitIndex-й бит NodeID в 0 или 1.
func setBit(id *model.NodeID, bitIndex int, value byte) {
	byteIndex := bitIndex / 8
	offset := uint(7 - (bitIndex % 8))
	mask := byte(1 << offset)

	if value == 0 {
		id[byteIndex] &^= mask
		return
	}

	id[byteIndex] |= mask
}
