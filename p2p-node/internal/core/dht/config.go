// p2p-node/internal/core/dht/config.go
package dht

import (
	"errors"
	"fmt"
	"time"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

const (
	// IDLengthBits — размер пространства идентификаторов Kademlia.
	//
	// В проекте NodeID = 32 байта = 256 бит.
	IDLengthBits = model.NodeIDBytes * 8

	// DefaultBucketSize — значение k для k-bucket.
	//
	// Для текущего прототипа используем k = 16.
	// Это соответствует твоей архитектурной спецификации для закрытого контура:
	// десятки/сотни узлов, умеренный размер таблицы, достаточная устойчивость.
	DefaultBucketSize = 16

	// DefaultAlpha — степень параллелизма iterative lookup.
	//
	// alpha = 3 — стандартное практическое значение для Kademlia.
	DefaultAlpha = 3

	// DefaultRequestTimeout — базовый timeout на один DHT RPC.
	DefaultRequestTimeout = 5 * time.Second

	// DefaultBucketRefreshInterval — интервал refresh k-bucket.
	DefaultBucketRefreshInterval = time.Hour

	// DefaultRecordTTL — время жизни DHT-записи.
	DefaultRecordTTL = 24 * time.Hour

	// DefaultReplicateInterval — интервал репликации записей.
	DefaultReplicateInterval = time.Hour

	// DefaultRepublishInterval — интервал повторной публикации владельцем.
	//
	// Должен быть меньше RecordTTL, чтобы запись не успевала протухнуть
	// до следующей публикации.
	DefaultRepublishInterval = 23 * time.Hour

	// DefaultMaxFailures — число подряд идущих сетевых ошибок, после которого
	// peer может считаться подозрительным кандидатом на удаление.
	DefaultMaxFailures = 3
)

// K оставлен как короткий alias для старого кода и тестов,
// где могло использоваться dht.K.
//
// В новой логике лучше использовать Config.K.
const K = DefaultBucketSize

// idLengthBits оставлен для совместимости с текущим внутренним стилем пакета.
const idLengthBits = IDLengthBits

var (
	ErrInvalidConfig      = errors.New("dht: invalid config")
	ErrNilPeer            = errors.New("dht: nil peer")
	ErrSelfPeer           = errors.New("dht: cannot add self peer")
	ErrInvalidBucketIndex = errors.New("dht: invalid bucket index")
	ErrPeerRejected       = errors.New("dht: peer rejected")
)

// Config задаёт параметры Kademlia DHT.
//
// Этот конфиг относится к доменной логике DHT:
//   - размер k-bucket;
//   - alpha-параллелизм;
//   - TTL/refresh/republish интервалы;
//   - политика допуска peer в routing table.
//
// Важно: здесь нет TCP/UDP/Protobuf/crypto-конкретики.
// Эти детали должны жить в infrastructure/usecase слоях.
type Config struct {
	// K — максимальное число peer в одном k-bucket.
	K int

	// Alpha — число параллельных запросов в iterative lookup.
	Alpha int

	// RequestTimeout — timeout одного DHT RPC.
	RequestTimeout time.Duration

	// BucketRefreshInterval — интервал обслуживания k-bucket.
	BucketRefreshInterval time.Duration

	// RecordTTL — срок жизни DHT-записи.
	RecordTTL time.Duration

	// ReplicateInterval — как часто узел реплицирует хранимые записи.
	ReplicateInterval time.Duration

	// RepublishInterval — как часто владелец записи повторно публикует её.
	RepublishInterval time.Duration

	// MaxFailures — сколько подряд сетевых отказов допускается для peer.
	MaxFailures uint32

	// EnforceTrustedPeers — если true, routing table будет принимать только
	// Peer.Trusted == true.
	//
	// Для раннего прототипа лучше оставить false, иначе bootstrap/simulation
	// будет сложнее. Для закрытого контура КИИ это поле пригодится позже.
	EnforceTrustedPeers bool
}

// DefaultConfig возвращает безопасные значения по умолчанию для DHT.
func DefaultConfig() Config {
	return Config{
		K:                     DefaultBucketSize,
		Alpha:                 DefaultAlpha,
		RequestTimeout:        DefaultRequestTimeout,
		BucketRefreshInterval: DefaultBucketRefreshInterval,
		RecordTTL:             DefaultRecordTTL,
		ReplicateInterval:     DefaultReplicateInterval,
		RepublishInterval:     DefaultRepublishInterval,
		MaxFailures:           DefaultMaxFailures,
		EnforceTrustedPeers:   false,
	}
}

// Normalize заполняет нулевые/отрицательные значения дефолтами.
//
// Это удобно, если конфиг частично собирается из config-файла.
func (c Config) Normalize() Config {
	def := DefaultConfig()

	if c.K <= 0 {
		c.K = def.K
	}
	if c.Alpha <= 0 {
		c.Alpha = def.Alpha
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = def.RequestTimeout
	}
	if c.BucketRefreshInterval <= 0 {
		c.BucketRefreshInterval = def.BucketRefreshInterval
	}
	if c.RecordTTL <= 0 {
		c.RecordTTL = def.RecordTTL
	}
	if c.ReplicateInterval <= 0 {
		c.ReplicateInterval = def.ReplicateInterval
	}
	if c.RepublishInterval <= 0 {
		c.RepublishInterval = def.RepublishInterval
	}
	if c.MaxFailures == 0 {
		c.MaxFailures = def.MaxFailures
	}

	return c
}

// Validate проверяет, что Config не противоречит логике Kademlia.
func (c Config) Validate() error {
	if c.K <= 0 {
		return fmt.Errorf("%w: K must be positive", ErrInvalidConfig)
	}
	if c.K > 1024 {
		return fmt.Errorf("%w: K is too large: %d", ErrInvalidConfig, c.K)
	}
	if c.Alpha <= 0 {
		return fmt.Errorf("%w: Alpha must be positive", ErrInvalidConfig)
	}
	if c.Alpha > c.K {
		return fmt.Errorf("%w: Alpha (%d) must not be greater than K (%d)", ErrInvalidConfig, c.Alpha, c.K)
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("%w: RequestTimeout must be positive", ErrInvalidConfig)
	}
	if c.BucketRefreshInterval <= 0 {
		return fmt.Errorf("%w: BucketRefreshInterval must be positive", ErrInvalidConfig)
	}
	if c.RecordTTL <= 0 {
		return fmt.Errorf("%w: RecordTTL must be positive", ErrInvalidConfig)
	}
	if c.ReplicateInterval <= 0 {
		return fmt.Errorf("%w: ReplicateInterval must be positive", ErrInvalidConfig)
	}
	if c.RepublishInterval <= 0 {
		return fmt.Errorf("%w: RepublishInterval must be positive", ErrInvalidConfig)
	}
	if c.RepublishInterval >= c.RecordTTL {
		return fmt.Errorf(
			"%w: RepublishInterval (%s) must be less than RecordTTL (%s)",
			ErrInvalidConfig,
			c.RepublishInterval,
			c.RecordTTL,
		)
	}
	if c.MaxFailures == 0 {
		return fmt.Errorf("%w: MaxFailures must be positive", ErrInvalidConfig)
	}

	return nil
}
