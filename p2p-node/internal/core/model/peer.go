package model

import (
	"time"
)

// Peer представляет узел в DHT-сети.
type Peer struct {
	ID NodeID

	// Addrs - список сетевых адресов. Использование []string позволяет ядру не зависеть от go-multiaddr.
	// Парсинг будет происходить во внешнем слое (инфраструктуре).
	Addrs []string

	// PubKey - храним сырые байты криптографического ключа ГОСТ, а не конкретный интерфейс (crypto.PublicKey)
	PubKey []byte

	lastSeen time.Time
}

// NewPeer создает новый экземпляр Peer.
func NewPeer(id NodeID, addrs []string, pubKey []byte) *Peer {
	return &Peer{
		ID:       id,
		Addrs:    addrs,
		PubKey:   pubKey,
		lastSeen: time.Now(),
	}
}

func (p *Peer) Seen() {
	p.lastSeen = time.Now()
}

func (p *Peer) LastSeen() time.Time {
	return p.lastSeen
}
