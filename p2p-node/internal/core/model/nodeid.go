// internal/core/model/nodeid.go
package model

import (
	"math/bits"
)

const (
	// NodeIDBytes - это длина NodeID в байтах ПО ГОСТ 256. ГОСТ 34.11-2012(вроде)
	NodeIDBytes = 32
)

// NodeID представляет 256-битный идентификатор узла в сети Kademlia.
type NodeID [NodeIDBytes]byte

// XORDistance вычисляет XOR-расстояние между двумя NodeID.
// Это расстояние используется для определения близости узлов.
func (n NodeID) XORDistance(other NodeID) NodeID {
	var result NodeID
	for i := 0; i < NodeIDBytes; i++ {
		result[i] = n[i] ^ other[i]
	}
	return result
}

// CommonPrefixLen возвращает длину общего префикса между двумя NodeID.
// Это значение используется для определения, в какую k-корзину поместить пир.
func (n NodeID) CommonPrefixLen(other NodeID) int {
	for i := 0; i < NodeIDBytes; i++ {
		if n[i] != other[i] {
			// Находим первый отличающийся бит в байте.
			return i*8 + bits.LeadingZeros8(n[i]^other[i])
		}
	}
	return NodeIDBytes * 8
}
