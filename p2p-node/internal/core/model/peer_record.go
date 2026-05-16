package model

import "time"

// PeerRecord содержит информацию о пире для публикации в DHT.
type PeerRecord struct {
	PeerID    NodeID    // Используем наш тип NodeID (32 байта)
	Addrs     []string  // Сетевые адреса (без привязки к multiaddr)
	Seq       uint64    // Защита от replay-атак
	Timestamp time.Time // Время создания записи
}
