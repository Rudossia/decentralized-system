// p2p-node/internal/core/model/peer.go
package model

import (
	"sort"
	"time"
)

// Peer представляет живой контакт другого узла, известный локальному узлу.
//
// Peer используется внутри routing table / k-buckets.
// Это НЕ обязательно публикуемая DHT-запись. Для публикации в DHT
// используется PeerRecord.
//
// Важное разделение:
//
//	Peer:
//	  - runtime-состояние контакта;
//	  - LastSeen, FailureCount, Trusted;
//	  - используется routing table.
//
//	PeerRecord:
//	  - сериализуемая запись для публикации;
//	  - Seq, Timestamp, Signature;
//	  - используется STORE/FIND_VALUE.
//
// В ядре проекта адреса оставлены как []string, чтобы core/model
// не зависел от go-multiaddr или конкретной transport-библиотеки.
// Валидация/парсинг multiaddr должна выполняться на infrastructure-слое.
type Peer struct {
	// ID — 256-битный идентификатор узла.
	ID NodeID
	// Addrs — сетевые адреса узла.
	//
	// Формат строки намеренно не фиксируется в core/model.
	// Это может быть:
	//   - "/ip4/127.0.0.1/tcp/9001"
	//   - "127.0.0.1:9001"
	//   - другой формат, выбранный infrastructure-слоем.
	Addrs []string
	// PubKey — байтовое представление публичного ключа узла.
	//
	// В core/model не используется crypto.PublicKey, чтобы доменная модель
	// не зависела от конкретной криптобиблиотеки, ГОСТ-реализации или PKI-адаптера.
	PubKey []byte
	// Capabilities — список поддерживаемых возможностей узла.
	// Например: - "dht" - "tcp"  - "udp"- "gost-sign"   - "store"
	Capabilities []string
	// ProtocolVersion — версия протокола, с которой был замечен peer.
	ProtocolVersion uint32
	// Agent — опциональная строка реализации узла.
	// Например: "p2p-node/0.1.0".
	//  для диагностики и аудита, но не должно использоваться
	// как доверенный security-фактор.
	Agent string
	// FirstSeen — когда узел впервые был добавлен в локальную таблицу.
	FirstSeen time.Time
	// lastSeen — когда узел последний раз проявил активность.
	// Поле закрыто, чтобы обновление происходило через Seen/Touch.
	lastSeen time.Time
	// FailureCount — количество подряд зафиксированных неудачных обращений.
	// Используется eviction-логикой и обслуживанием routing table.
	FailureCount uint32
	// Trusted — признак, что peer прошёл процедуру доверенного допуска.
	// Для закрытого контура КИИ это важно: Kademlia-таблица не должна бесконтрольно наполняться недоверенными узлами.
	// На ранней стадии поле может не использоваться, но оно задаёт правильную точку расширения.
	Trusted bool
}

// NewPeer создаёт новый Peer.
// NewPeer(id, addrs, pubKey) *Peer.
//
// Дополнительные поля можно установить через методы:
//   - SetCapabilities
//   - SetProtocolVersion
//   - SetAgent
//   - MarkTrusted
func NewPeer(id NodeID, addrs []string, pubKey []byte) *Peer {
	now := time.Now().UTC()

	return &Peer{
		ID:              id,
		Addrs:           normalizeStringSet(addrs),
		PubKey:          cloneBytes(pubKey),
		Capabilities:    nil,
		ProtocolVersion: 0,
		Agent:           "",
		FirstSeen:       now,
		lastSeen:        now,
		FailureCount:    0,
		Trusted:         false,
	}
}

// NewPeerFull создаёт Peer с полным набором основных параметров.
//
// Его удобно использовать в тестах, bootstrap-конфигурации и при сборке peer
// после успешного handshake.
func NewPeerFull(id NodeID, addrs []string, pubKey []byte, capabilities []string, protocolVersion uint32, agent string, trusted bool) *Peer {
	p := NewPeer(id, addrs, pubKey)
	p.Capabilities = normalizeStringSet(capabilities)
	p.ProtocolVersion = protocolVersion
	p.Agent = agent
	p.Trusted = trusted
	return p
}

// Seen отмечает, что peer успешно проявил активность прямо сейчас.
// Это используется при:
//   - получении PING/PONG;
//   - успешном FIND_NODE;
//   - успешном FIND_VALUE;
//   - успешном STORE;
//   - любом валидном входящем сообщении от этого узла.
func (p *Peer) Seen() {
	p.Touch(time.Now().UTC())
}

// Touch обновляет LastSeen заданным временем.
// Этот метод удобен для тестов, потому что позволяет задавать время явно.
func (p *Peer) Touch(t time.Time) {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	p.lastSeen = t.UTC()
	p.FailureCount = 0
}

// LastSeen возвращает время последней успешной активности peer.
func (p *Peer) LastSeen() time.Time {
	return p.lastSeen
}

// MarkFailure увеличивает счётчик подряд идущих неудачных обращений.
// Например, если PING/FIND_NODE не получил ответа или завершился timeout.
func (p *Peer) MarkFailure() {
	p.FailureCount++
}

// ResetFailures сбрасывает счётчик ошибок.
func (p *Peer) ResetFailures() {
	p.FailureCount = 0
}

// SetAddresses заменяет список адресов peer.
func (p *Peer) SetAddresses(addrs []string) {
	p.Addrs = normalizeStringSet(addrs)
}

// AddAddress добавляет новый адрес, если его ещё нет.
func (p *Peer) AddAddress(addr string) {
	if addr == "" {
		return
	}
	for _, existing := range p.Addrs {
		if existing == addr {
			return
		}
	}
	p.Addrs = append(p.Addrs, addr)
	sort.Strings(p.Addrs)
}

// HasAddress проверяет, известен ли peer по указанному адресу.
func (p *Peer) HasAddress(addr string) bool {
	for _, existing := range p.Addrs {
		if existing == addr {
			return true
		}
	}
	return false
}

// SetPublicKey заменяет публичный ключ peer.
func (p *Peer) SetPublicKey(pubKey []byte) {
	p.PubKey = cloneBytes(pubKey)
}

// SetCapabilities заменяет список возможностей peer.
func (p *Peer) SetCapabilities(capabilities []string) {
	p.Capabilities = normalizeStringSet(capabilities)
}

// Supports проверяет, заявлена ли у peer возможность capability.
func (p *Peer) Supports(capability string) bool {
	if capability == "" {
		return false
	}
	for _, c := range p.Capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

// SetProtocolVersion задаёт версию протокола, с которой был замечен peer.
func (p *Peer) SetProtocolVersion(version uint32) {
	p.ProtocolVersion = version
}

// SetAgent задаёт диагностическую строку реализации peer.
func (p *Peer) SetAgent(agent string) {
	p.Agent = agent
}

// MarkTrusted помечает peer как прошедший доверенную процедуру допуска.
func (p *Peer) MarkTrusted() {
	p.Trusted = true
}

// MarkUntrusted снимает признак доверенного peer.
func (p *Peer) MarkUntrusted() {
	p.Trusted = false
}

// IsStale проверяет, не устарел ли peer относительно заданного времени.
//
// Например, можно считать peer устаревшим, если LastSeen старше refresh interval.
func (p *Peer) IsStale(now time.Time, maxIdle time.Duration) bool {
	if maxIdle <= 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.UTC().Sub(p.lastSeen) > maxIdle
}

// Clone возвращает глубокую копию Peer.
// Это важно, чтобы routing table или внешние обработчики не изменяли общий
// объект через указатель случайно.
func (p *Peer) Clone() *Peer {
	if p == nil {
		return nil
	}
	return &Peer{
		ID:              p.ID,
		Addrs:           cloneStrings(p.Addrs),
		PubKey:          cloneBytes(p.PubKey),
		Capabilities:    cloneStrings(p.Capabilities),
		ProtocolVersion: p.ProtocolVersion,
		Agent:           p.Agent,
		FirstSeen:       p.FirstSeen,
		lastSeen:        p.lastSeen,
		FailureCount:    p.FailureCount,
		Trusted:         p.Trusted,
	}
}

// ToPeerRecord создаёт публикуемую DHT-запись на основе Peer.
//   - LastSeen переносится как runtime-метаданные;
//   - подпись здесь не создаётся;
//   - подпись должна быть наложена отдельным security/usecase-слоем
//     через PeerRecord.SigningBytes().
func (p *Peer) ToPeerRecord(seq uint64, ttl time.Duration) PeerRecord {
	if p == nil {
		return PeerRecord{}
	}
	now := time.Now().UTC()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	return PeerRecord{
		NodeID:          p.ID,
		Addrs:           cloneStrings(p.Addrs),
		PublicKey:       cloneBytes(p.PubKey),
		Capabilities:    cloneStrings(p.Capabilities),
		ProtocolVersion: p.ProtocolVersion,
		Agent:           p.Agent,
		Seq:             seq,
		CreatedAt:       now,
		UpdatedAt:       now,
		ExpiresAt:       expiresAt,
		LastSeen:        p.LastSeen(),
		SignatureAlg:    "",
		Signature:       nil,
	}
}

// normalizeStringSet удаляет пустые строки, дубликаты и сортирует результат.
//   - стабильного сравнения в тестах;
//   - стабильного signing input;
//   - одинакового представления адресов/capabilities.
func normalizeStringSet(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	sort.Strings(result)
	return result
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cp := make([]string, len(values))
	copy(cp, values)
	return cp
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	return cp
}
