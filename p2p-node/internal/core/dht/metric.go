package dht

import (
	"bytes"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// ByDistance реализует sort.Interface для сортировки пиров по XOR-расстоянию.
type ByDistance struct {
	Peers  []*model.Peer
	Target model.NodeID
}

func (a ByDistance) Len() int {
	return len(a.Peers)
}

func (a ByDistance) Swap(i, j int) {
	a.Peers[i],
		a.Peers[j] = a.Peers[j],
		a.Peers[i]
}

func (a ByDistance) Less(i, j int) bool {
	distI := a.Peers[i].ID.XORDistance(a.Target)
	distJ := a.Peers[j].ID.XORDistance(a.Target)
	// Сравниваем байты напрямую, без перевода в строку
	return bytes.Compare(distI[:], distJ[:]) < 0
}
