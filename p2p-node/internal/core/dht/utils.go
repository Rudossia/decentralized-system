// p2p-node/internal/core/dht/util.go
package dht

import "github.com/Rudossia/p2p-node/internal/core/model"

// cloneBytes возвращает глубокую копию []byte.
//
// Нужна внутри пакета dht, потому что model.cloneBytes не экспортируется
// и недоступна из другого пакета.
func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}

	cp := make([]byte, len(value))
	copy(cp, value)
	return cp
}

// cloneNodeIDs возвращает глубокую копию []model.NodeID.
func cloneNodeIDs(values []model.NodeID) []model.NodeID {
	if len(values) == 0 {
		return nil
	}

	cp := make([]model.NodeID, len(values))
	copy(cp, values)
	return cp
}
