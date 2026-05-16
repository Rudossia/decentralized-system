// p2p-node/internal/core/dht/lookup_result.go
package dht

import (
	"errors"

	"github.com/Rudossia/p2p-node/internal/core/model"
)

// CandidateState — состояние кандидата в iterative lookup.
type CandidateState int

const (
	CandidateUnknown CandidateState = iota
	CandidateUnqueried
	CandidateQuerying
	CandidateQueried
	CandidateFailed
)

func (s CandidateState) String() string {
	switch s {
	case CandidateUnqueried:
		return "unqueried"
	case CandidateQuerying:
		return "querying"
	case CandidateQueried:
		return "queried"
	case CandidateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Candidate — один peer-кандидат в shortlist iterative lookup.
type Candidate struct {
	Peer  *model.Peer
	State CandidateState

	// Err хранит ошибку последнего обращения к peer.
	Err error
}

// Clone возвращает глубокую копию Candidate.
func (c Candidate) Clone() Candidate {
	return Candidate{
		Peer:  c.Peer.Clone(),
		State: c.State,
		Err:   c.Err,
	}
}

// LookupStep — один шаг iterative lookup.
//
// Эта структура нужна не только для отладки,
// но и для будущей проверки логарифмичности поиска:
// по Trace можно строить график "итерация -> лучшее XOR-расстояние".
type LookupStep struct {
	Iteration int

	// Queried — кого опрашивали на этом шаге.
	Queried []model.NodeID

	// Discovered — каких новых peer получили в ответах.
	Discovered []model.NodeID

	// BestPeer — лучший известный peer после шага.
	BestPeer model.NodeID

	// BestDistance — XOR-расстояние от BestPeer до Target.
	BestDistance model.NodeID

	// ShortlistSize — размер shortlist после обработки ответов.
	ShortlistSize int

	// FailedRequests — сколько запросов завершилось ошибкой на этом шаге.
	FailedRequests int
}

// Clone возвращает глубокую копию LookupStep.
func (s LookupStep) Clone() LookupStep {
	return LookupStep{
		Iteration:      s.Iteration,
		Queried:        cloneNodeIDs(s.Queried),
		Discovered:     cloneNodeIDs(s.Discovered),
		BestPeer:       s.BestPeer,
		BestDistance:   s.BestDistance,
		ShortlistSize:  s.ShortlistSize,
		FailedRequests: s.FailedRequests,
	}
}

// LookupResult — результат IterativeFindNode.
type LookupResult struct {
	Target model.NodeID

	// Closest — итоговый набор ближайших peer.
	Closest []*model.Peer

	// Iterations — сколько раундов lookup было выполнено.
	Iterations int

	// RPCRequests — сколько RPC-запросов было отправлено.
	RPCRequests int

	// FailedRequests — сколько RPC-запросов завершилось ошибкой.
	FailedRequests int

	// Trace — подробная история поиска.
	Trace []LookupStep
}

// AddStep добавляет шаг в trace.
func (r *LookupResult) AddStep(step LookupStep) {
	if r == nil {
		return
	}

	r.Trace = append(r.Trace, step.Clone())
}

// Success возвращает true, если lookup нашёл хотя бы одного peer.
func (r LookupResult) Success() bool {
	return len(r.Closest) > 0
}

// Clone возвращает глубокую копию LookupResult.
func (r LookupResult) Clone() LookupResult {
	closest := make([]*model.Peer, 0, len(r.Closest))
	for _, peer := range r.Closest {
		if peer == nil {
			continue
		}
		closest = append(closest, peer.Clone())
	}

	trace := make([]LookupStep, 0, len(r.Trace))
	for _, step := range r.Trace {
		trace = append(trace, step.Clone())
	}

	return LookupResult{
		Target:         r.Target,
		Closest:        closest,
		Iterations:     r.Iterations,
		RPCRequests:    r.RPCRequests,
		FailedRequests: r.FailedRequests,
		Trace:          trace,
	}
}

// FindValueResult — результат FIND_VALUE.
//
// Если Found == true, тогда:
//   - PeerRecord может быть заполнен, если искали запись о пире;
//   - Value может быть заполнен, если искали обычное Key -> Value.
//
// Если Found == false, тогда Closest содержит ближайших известных peer,
// как в Kademlia FIND_VALUE.
type FindValueResult struct {
	Key model.NodeID

	Found bool

	PeerRecord *model.PeerRecord
	Value      []byte

	Closest []*model.Peer
}

// Clone возвращает глубокую копию FindValueResult.
func (r FindValueResult) Clone() FindValueResult {
	closest := make([]*model.Peer, 0, len(r.Closest))
	for _, peer := range r.Closest {
		if peer == nil {
			continue
		}
		closest = append(closest, peer.Clone())
	}

	var peerRecord *model.PeerRecord
	if r.PeerRecord != nil {
		cp := r.PeerRecord.Clone()
		peerRecord = &cp
	}

	return FindValueResult{
		Key:        r.Key,
		Found:      r.Found,
		PeerRecord: peerRecord,
		Value:      cloneBytes(r.Value),
		Closest:    closest,
	}
}

// StoreResult — результат IterativeStore/Publish.
type StoreResult struct {
	Key model.NodeID

	TargetPeers []*model.Peer

	Stored int
	Failed int

	Errors []error
}

// AddError добавляет ошибку в StoreResult.
func (r *StoreResult) AddError(err error) {
	if r == nil || err == nil {
		return
	}

	r.Errors = append(r.Errors, err)
	r.Failed++
}

// Error возвращает объединённую ошибку по StoreResult.
func (r StoreResult) Error() error {
	if len(r.Errors) == 0 {
		return nil
	}

	return errors.Join(r.Errors...)
}

// Clone возвращает глубокую копию StoreResult.
func (r StoreResult) Clone() StoreResult {
	targets := make([]*model.Peer, 0, len(r.TargetPeers))
	for _, peer := range r.TargetPeers {
		if peer == nil {
			continue
		}
		targets = append(targets, peer.Clone())
	}

	errorsCopy := make([]error, len(r.Errors))
	copy(errorsCopy, r.Errors)

	return StoreResult{
		Key:         r.Key,
		TargetPeers: targets,
		Stored:      r.Stored,
		Failed:      r.Failed,
		Errors:      errorsCopy,
	}
}

func cloneNodeIDs(values []model.NodeID) []model.NodeID {
	if len(values) == 0 {
		return nil
	}

	cp := make([]model.NodeID, len(values))
	copy(cp, values)
	return cp
}
