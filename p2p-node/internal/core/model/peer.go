// internal/core/model/peer.go
package model

import (
	"crypto"
	"time"

	"github.com/multiformats/go-multiaddr"
)

// Peer представляет узел в DHT-сети.
type Peer struct {
	// ID - уникальный идентификатор узла.
	ID NodeID
	// Addrs - список сетевых адресов, по которым можно связаться с пиром.
	// Используется multiaddr для гибкости.
	Addrs []multiaddr.Multiaddr
	// PubKey - криптографический публичный ключ пира.
	// Используется для верификации его личности.
	PubKey crypto.PublicKey
	// lastSeen - время последнего успешного контакта с пиром.
	// Это поле используется для логики вытеснения из k-корзин.
	lastSeen time.Time
}

// NewPeer создает новый экземпляр Peer.
func NewPeer(id NodeID, addrs []multiaddr.Multiaddr, pubKey crypto.PublicKey) *Peer {
	return &Peer{
		ID:       id,
		Addrs:    addrs,
		PubKey:   pubKey,
		lastSeen: time.Now(),
	}
}

// Seen обновляет время последнего контакта.
func (p *Peer) Seen() {
	p.lastSeen = time.Now()
}

// LastSeen возвращает время последнего контакта.
func (p *Peer) LastSeen() time.Time {
	return p.lastSeen
}
