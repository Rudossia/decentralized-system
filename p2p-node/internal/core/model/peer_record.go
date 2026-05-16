// p2p-node/internal/core/model/peer_record.go
package model

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrPeerRecordEmptyNodeID = errors.New("peer record: empty node id")
	ErrPeerRecordNoAddresses = errors.New("peer record: no addresses")
	ErrPeerRecordExpired     = errors.New("peer record: expired")
	ErrPeerRecordBadSequence = errors.New("peer record: bad sequence")
)

// PeerRecord — публикуемая DHT-запись о пире.
//
// В отличие от Peer, эта структура предназначена для передачи/хранения
// через DHT:
//   - STORE(NodeID -> PeerRecord)
//   - FIND_VALUE(NodeID)
//   - репликация записи на k ближайших узлов
//   - подпись записи владельцем узла
//
// PeerRecord должен быть пригоден для последующей криптографической защиты. Поэтому здесь уже есть поля SignatureAlg и Signature,
// но сама криптографическая операция НЕ выполняется в model.
// Почему подпись не здесь:
//   - model не должен зависеть от конкретного ГОСТ/PKCS#11/СКЗИ;
//   - подпись/проверка должны быть в security/usecase/infrastructure-слое;
//   - model только формирует стабильный SigningBytes().
type PeerRecord struct {
	// NodeID — 256-битный идентификатор узла, которому принадлежит запись.
	NodeID NodeID
	// Addrs — адреса, по которым узел может быть доступен.
	// Формат не фиксируется на уровне core/model.
	Addrs []string
	// PublicKey — публичный ключ узла в байтовом представлении.
	// В прототипе это может быть DER/SPKI/сырой ключ.
	// В production-варианте поле может быть заменено на certificate reference или цепочку сертификатов, если идентичность подтверждается через PKI.
	PublicKey []byte
	// Capabilities — возможности узла.
	// Например:
	//   - "dht"
	//   - "tcp"
	//   - "udp"
	//   - "store"
	//   - "gost-sign"
	Capabilities []string
	// ProtocolVersion — версия протокола, поддерживаемая узлом.
	ProtocolVersion uint32
	// Agent — диагностическая строка реализации.
	Agent string
	// Seq — монотонно возрастающий номер версии записи.
	// Нужен для защиты от replay-атак:
	// если злоумышленник попытается вернуть старую, но корректно подписанную
	// запись, узел сможет сравнить Seq и оставить более новую версию.
	Seq uint64
	// CreatedAt — время первичного создания записи.
	CreatedAt time.Time
	// UpdatedAt — время последнего обновления содержимого записи.
	UpdatedAt time.Time
	// ExpiresAt — время истечения срока действия записи.
	//
	// Если zero-value, то срок действия не задан.
	// Для DHT лучше задавать TTL явно.
	ExpiresAt time.Time
	// LastSeen — runtime-метаданные о последней активности peer.
	//
	// Важное замечание:
	// LastSeen НЕ должен входить в SigningBytes(), потому что это локально
	// наблюдаемое состояние, которое может отличаться на разных узлах.
	LastSeen time.Time
	// SignatureAlg — идентификатор алгоритма подписи.
	//
	// Например:
	//   - "gost3410-2012-256"
	//   - "prototype-ed25519"
	//
	// Конкретные OID/DER-представления лучше держать в security-слое.
	SignatureAlg string
	// Signature — подпись над SigningBytes().
	Signature []byte
}

// NewPeerRecord создаёт новую публикуемую DHT-запись.
// ttl <= 0 означает, что ExpiresAt не задаётся.  для DHT лучше задавать TTL явно.
func NewPeerRecord(nodeID NodeID, addrs []string, publicKey []byte, capabilities []string, protocolVersion uint32, agent string, seq uint64, ttl time.Duration) PeerRecord {
	now := time.Now().UTC()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	return PeerRecord{
		NodeID:          nodeID,
		Addrs:           normalizeStringSet(addrs),
		PublicKey:       cloneBytes(publicKey),
		Capabilities:    normalizeStringSet(capabilities),
		ProtocolVersion: protocolVersion,
		Agent:           agent,
		Seq:             seq,
		CreatedAt:       now,
		UpdatedAt:       now,
		ExpiresAt:       expiresAt,
		LastSeen:        time.Time{},
		SignatureAlg:    "",
		Signature:       nil,
	}
}

// NewPeerRecordFromPeer создаёт DHT-запись на основе живого Peer.
func NewPeerRecordFromPeer(peer *Peer, seq uint64, ttl time.Duration) PeerRecord {
	if peer == nil {
		return PeerRecord{}
	}
	return peer.ToPeerRecord(seq, ttl)
}

// Validate проверяет базовую корректность PeerRecord.
// now можно передать time.Now().UTC().
// Если now.IsZero(), проверка истечения срока действия не выполняется.
func (r PeerRecord) Validate(now time.Time) error {
	if isZeroNodeID(r.NodeID) {
		return ErrPeerRecordEmptyNodeID
	}
	if len(r.Addrs) == 0 {
		return ErrPeerRecordNoAddresses
	}
	if r.Seq == 0 {
		return ErrPeerRecordBadSequence
	}
	if !now.IsZero() && r.IsExpired(now) {
		return ErrPeerRecordExpired
	}
	return nil
}

// IsExpired проверяет истёк ли срок действия записи.
func (r PeerRecord) IsExpired(now time.Time) bool {
	if r.ExpiresAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return !now.UTC().Before(r.ExpiresAt.UTC())
}

// NewerThan сравнивает две записи одного и того же peer.
// Возвращает true, если текущая запись должна считаться более новой.
func (r PeerRecord) NewerThan(other PeerRecord) bool {
	if r.Seq != other.Seq {
		return r.Seq > other.Seq
	}
	return r.UpdatedAt.After(other.UpdatedAt)
}

// BumpSeq увеличивает Seq и обновляет UpdatedAt.
// Используется владельцем записи перед повторной публикацией.
func (r *PeerRecord) BumpSeq() {
	r.Seq++
	r.UpdatedAt = time.Now().UTC()
	r.SignatureAlg = ""
	r.Signature = nil
}

// RefreshTTL продлевает срок жизни записи.
func (r *PeerRecord) RefreshTTL(ttl time.Duration) {
	now := time.Now().UTC()
	r.UpdatedAt = now
	if ttl > 0 {
		r.ExpiresAt = now.Add(ttl)
	}
	// После изменения подписываемых полей старая подпись недействительна.
	r.SignatureAlg = ""
	r.Signature = nil
}

// SetAddresses заменяет адреса записи.
func (r *PeerRecord) SetAddresses(addrs []string) {
	r.Addrs = normalizeStringSet(addrs)
	r.UpdatedAt = time.Now().UTC()
	r.SignatureAlg = ""
	r.Signature = nil
}

// SetCapabilities заменяет capabilities записи.
func (r *PeerRecord) SetCapabilities(capabilities []string) {
	r.Capabilities = normalizeStringSet(capabilities)
	r.UpdatedAt = time.Now().UTC()
	r.SignatureAlg = ""
	r.Signature = nil
}

// AttachSignature прикрепляет подпись к записи.
// Предполагается, что signature уже вычислена над r.SigningBytes().
func (r *PeerRecord) AttachSignature(signatureAlg string, signature []byte) {
	r.SignatureAlg = signatureAlg
	r.Signature = cloneBytes(signature)
}

// ClearSignature удаляет подпись.
// Полезно в тестах или перед повторным подписанием.
func (r *PeerRecord) ClearSignature() {
	r.SignatureAlg = ""
	r.Signature = nil
}

// HasSignature проверяет наличие подписи.
func (r PeerRecord) HasSignature() bool {
	return r.SignatureAlg != "" && len(r.Signature) > 0
}

// ToPeer преобразует PeerRecord в runtime-контакт Peer./
// При восстановлении Peer из PeerRecord:
//   - FirstSeen и LastSeen задаются текущим временем;
//   - Trusted выставляется в false;
//   - доверенность должна устанавливаться после проверки подписи/сертификата.
func (r PeerRecord) ToPeer() *Peer {
	p := NewPeerFull(
		r.NodeID,
		r.Addrs,
		r.PublicKey,
		r.Capabilities,
		r.ProtocolVersion,
		r.Agent,
		false,
	)
	if !r.LastSeen.IsZero() {
		p.Touch(r.LastSeen)
	}
	return p
}

// Clone возвращает глубокую копию PeerRecord.
func (r PeerRecord) Clone() PeerRecord {
	return PeerRecord{
		NodeID:          r.NodeID,
		Addrs:           cloneStrings(r.Addrs),
		PublicKey:       cloneBytes(r.PublicKey),
		Capabilities:    cloneStrings(r.Capabilities),
		ProtocolVersion: r.ProtocolVersion,
		Agent:           r.Agent,
		Seq:             r.Seq,
		CreatedAt:       r.CreatedAt,
		UpdatedAt:       r.UpdatedAt,
		ExpiresAt:       r.ExpiresAt,
		LastSeen:        r.LastSeen,
		SignatureAlg:    r.SignatureAlg,
		Signature:       cloneBytes(r.Signature),
	}
}

// SigningBytes возвращает стабильное байтовое представление записи, предназначенное для подписи.
// В SigningBytes входят только поля, описывающие публикуемую DHT-запись.
// НЕ входят:
//   - SignatureAlg;
//   - Signature;
//   - LastSeen.
//
// LastSeen — локальное наблюдение конкретного узла. Если включить его в подпись, разные узлы будут иметь разные версии одной и той же записи,
// что разрушит нормальную репликацию DHT.
func (r PeerRecord) SigningBytes() ([]byte, error) {
	input := peerRecordSigningInput{
		NodeIDHex:       hex.EncodeToString(r.NodeID[:]),
		Addrs:           normalizeStringSet(r.Addrs),
		PublicKeyHex:    hex.EncodeToString(r.PublicKey),
		Capabilities:    normalizeStringSet(r.Capabilities),
		ProtocolVersion: r.ProtocolVersion,
		Agent:           r.Agent,
		Seq:             r.Seq,
		CreatedAtUnixMs: unixMillis(r.CreatedAt),
		UpdatedAtUnixMs: unixMillis(r.UpdatedAt),
		ExpiresAtUnixMs: unixMillis(r.ExpiresAt),
	}
	return json.Marshal(input)
}

// peerRecordSigningInput — внутреннее стабильное представление для подписи.
// Используется отдельная структура, чтобы случайно не подписать runtime-поля.
type peerRecordSigningInput struct {
	NodeIDHex       string   `json:"node_id_hex"`
	Addrs           []string `json:"addrs,omitempty"`
	PublicKeyHex    string   `json:"public_key_hex,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
	ProtocolVersion uint32   `json:"protocol_version"`
	Agent           string   `json:"agent,omitempty"`
	Seq             uint64   `json:"seq"`
	CreatedAtUnixMs int64    `json:"created_at_unix_ms"`
	UpdatedAtUnixMs int64    `json:"updated_at_unix_ms"`
	ExpiresAtUnixMs int64    `json:"expires_at_unix_ms,omitempty"`
}

func unixMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano() / int64(time.Millisecond)
}
func isZeroNodeID(id NodeID) bool {
	var zero NodeID
	return id == zero
}
