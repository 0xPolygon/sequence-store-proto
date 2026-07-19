// Package chainstate implements the protocol's chain state machine:
// structural validation, the head check, the commitment fold, and
// generation tracking. It is storage-agnostic — positions are logical
// entry indexes counted by the state itself — so every store
// implementation shares the one apply path, at ingest and at replay
// alike: the reference store composes it with an in-memory log, a
// production store with a replicated one.
//
// A State is not safe for concurrent use; callers serialize access.
package chainstate

import (
	"math/big"

	"github.com/0xPolygon/sequence-store-proto/commitment"
	pb "github.com/0xPolygon/sequence-store-proto/sequencestore/v1"
)

// Generation is one publication generation of a block height: the logical
// positions of its open, records, and seal among accepted entries. A
// re-anchor at the same height starts a new generation; block-addressed
// reads resolve to the latest.
type Generation struct {
	Positions []uint64
	Sealed    bool
}

// State holds one append-only chain guarded by the head check.
type State struct {
	seed    commitment.Head
	head    commitment.Head
	applied uint64

	gens         map[uint64]*Generation
	sealedHeight map[[32]byte]uint64 // sealed hash -> height, for known-parent checks
	openHeight   uint64
	openActive   bool
}

// New returns an empty state whose head is the genesis seed for chainID.
func New(chainID uint64) *State {
	seed := commitment.Seed(chainID)

	return &State{
		seed:         seed,
		head:         seed,
		gens:         map[uint64]*Generation{},
		sealedHeight: map[[32]byte]uint64{},
	}
}

// NewAt returns a state resuming at head instead of the genesis seed, for
// replayers adopting a trusted mid-chain position: a cold replay of a
// partially retained log skips to its first BlockOpen and adopts that
// open's prefix commitment — trustworthy because it was head-checked when
// appended. Generations and known parents below the adopted point are
// unknown to the state, matching what the log below the retention floor no
// longer carries.
func NewAt(chainID uint64, head commitment.Head) *State {
	s := New(chainID)
	s.head = head

	return s
}

// Seed returns the genesis seed the chain starts from.
func (s *State) Seed() commitment.Head {
	return s.seed
}

// Head returns the current commitment head.
func (s *State) Head() commitment.Head {
	return s.head
}

// Generation returns a copy of the latest generation at height.
func (s *State) Generation(height uint64) (Generation, bool) {
	gen, ok := s.gens[height]
	if !ok {
		return Generation{}, false
	}

	return Generation{
		Positions: append([]uint64(nil), gen.Positions...),
		Sealed:    gen.Sealed,
	}, true
}

// Apply runs the one ingest path for an entry: structural validation, the
// head check, per-kind rules, fold. On OK the state has advanced and the
// entry's logical position is the pre-call applied count; on any other
// status the state is unchanged. The caller owns storing the entry —
// an accepted entry mutated afterwards corrupts every replayer.
func (s *State) Apply(entry *pb.Entry) (commitment.Head, pb.AckStatus) {
	prefix, ok := validate(entry)
	if !ok {
		return commitment.Head{}, pb.AckStatus_ACK_STATUS_MALFORMED
	}

	if commitment.Head(prefix) != s.head {
		return commitment.Head{}, pb.AckStatus_ACK_STATUS_STALE_COMMITMENT
	}

	var (
		next   commitment.Head
		status pb.AckStatus
	)

	switch kind := entry.GetKind().(type) {
	case *pb.Entry_BlockOpen:
		next, status = s.applyOpen(kind.BlockOpen)
	case *pb.Entry_Record:
		next, status = s.applyRecord(kind.Record)
	case *pb.Entry_BlockSeal:
		next, status = s.applySeal(kind.BlockSeal)
	}

	if status != pb.AckStatus_ACK_STATUS_OK {
		return commitment.Head{}, status
	}

	s.head = next
	s.applied++

	return next, pb.AckStatus_ACK_STATUS_OK
}

func (s *State) applyOpen(open *pb.BlockOpen) (commitment.Head, pb.AckStatus) {
	// Whenever the named parent is store-known, the open's height must be
	// the parent's height + 1; an unknown parent is a forward jump and the
	// height is taken as claimed.
	parent := [32]byte(open.GetParentHash())
	if height, known := s.sealedHeight[parent]; known && open.GetBlockNumber() != height+1 {
		return commitment.Head{}, pb.AckStatus_ACK_STATUS_MALFORMED
	}

	next, err := commitment.FoldOpen(s.head, commitment.OpenContext{
		Number:     open.GetBlockNumber(),
		Timestamp:  open.GetBlockTimestamp(),
		ParentHash: parent,
		GasLimit:   open.GetGasLimit(),
		BaseFee:    new(big.Int).SetBytes(open.GetBaseFee()),
	})
	if err != nil {
		return commitment.Head{}, pb.AckStatus_ACK_STATUS_MALFORMED
	}

	s.openHeight = open.GetBlockNumber()
	s.openActive = true
	s.gens[s.openHeight] = &Generation{Positions: []uint64{s.applied}}

	return next, pb.AckStatus_ACK_STATUS_OK
}

func (s *State) applyRecord(record *pb.Record) (commitment.Head, pb.AckStatus) {
	if !s.openActive {
		return commitment.Head{}, pb.AckStatus_ACK_STATUS_MALFORMED
	}

	gen := s.gens[s.openHeight]
	gen.Positions = append(gen.Positions, s.applied)

	return commitment.FoldTxs(s.head, record.GetTransactions()), pb.AckStatus_ACK_STATUS_OK
}

func (s *State) applySeal(seal *pb.BlockSeal) (commitment.Head, pb.AckStatus) {
	if !s.openActive {
		return commitment.Head{}, pb.AckStatus_ACK_STATUS_MALFORMED
	}

	sealedHash := commitment.SealedHash(seal.GetHeader())

	gen := s.gens[s.openHeight]
	gen.Positions = append(gen.Positions, s.applied)
	gen.Sealed = true
	s.sealedHeight[sealedHash] = s.openHeight
	s.openActive = false

	return commitment.FoldSeal(s.head, sealedHash), pb.AckStatus_ACK_STATUS_OK
}

// validate applies the structural rules the MALFORMED status enumerates and
// returns the entry's prefix commitment.
func validate(entry *pb.Entry) ([]byte, bool) {
	switch kind := entry.GetKind().(type) {
	case *pb.Entry_BlockOpen:
		open := kind.BlockOpen
		if len(open.GetParentHash()) != 32 {
			return nil, false
		}

		return open.GetPrefixCommitment(), len(open.GetPrefixCommitment()) == 32
	case *pb.Entry_Record:
		record := kind.Record
		if len(record.GetTransactions()) == 0 {
			return nil, false
		}

		return record.GetPrefixCommitment(), len(record.GetPrefixCommitment()) == 32
	case *pb.Entry_BlockSeal:
		seal := kind.BlockSeal

		return seal.GetPrefixCommitment(), len(seal.GetPrefixCommitment()) == 32
	default:
		return nil, false
	}
}
