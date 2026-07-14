package chainstate

import (
	"math/big"
	"testing"

	"github.com/0xPolygon/sequence-store-proto/commitment"
	pb "github.com/0xPolygon/sequence-store-proto/sequencestore/v1"
)

const testChainID = 137

// chain builds valid entries producer-side, tracking the running head.
type chain struct {
	t    *testing.T
	head commitment.Head
}

func newChain(t *testing.T) *chain {
	t.Helper()

	return &chain{t: t, head: commitment.Seed(testChainID)}
}

func (c *chain) open(number uint64, parent [32]byte) *pb.Entry {
	c.t.Helper()

	ctx := commitment.OpenContext{
		Number:     number,
		Timestamp:  1750000000 + number,
		ParentHash: parent,
		GasLimit:   45000000,
		BaseFee:    big.NewInt(25000000000),
	}
	entry := &pb.Entry{Kind: &pb.Entry_BlockOpen{BlockOpen: &pb.BlockOpen{
		BlockNumber:      ctx.Number,
		BlockTimestamp:   ctx.Timestamp,
		ParentHash:       ctx.ParentHash[:],
		GasLimit:         ctx.GasLimit,
		BaseFee:          ctx.BaseFee.Bytes(),
		PrefixCommitment: c.head.Bytes(),
	}}}

	head, err := commitment.FoldOpen(c.head, ctx)
	if err != nil {
		c.t.Fatalf("FoldOpen: %v", err)
	}

	c.head = head

	return entry
}

func (c *chain) record(txs ...[]byte) *pb.Entry {
	entry := &pb.Entry{Kind: &pb.Entry_Record{Record: &pb.Record{
		Transactions:     txs,
		PrefixCommitment: c.head.Bytes(),
	}}}
	c.head = commitment.FoldTxs(c.head, txs)

	return entry
}

func (c *chain) seal(header []byte) (*pb.Entry, [32]byte) {
	entry := &pb.Entry{Kind: &pb.Entry_BlockSeal{BlockSeal: &pb.BlockSeal{
		Header:           header,
		PrefixCommitment: c.head.Bytes(),
	}}}
	sealed := commitment.SealedHash(header)
	c.head = commitment.FoldSeal(c.head, sealed)

	return entry, sealed
}

func mustApply(t *testing.T, s *State, entries ...*pb.Entry) {
	t.Helper()

	for _, entry := range entries {
		if _, status := s.Apply(entry); status != pb.AckStatus_ACK_STATUS_OK {
			t.Fatalf("Apply = %v, want OK", status)
		}
	}
}

func TestGenerationTracking(t *testing.T) {
	s := New(testChainID)
	c := newChain(t)

	mustApply(t, s, c.open(101, [32]byte{0xef}), c.record([]byte{0x01}))

	gen, ok := s.Generation(101)
	if !ok || gen.Sealed || len(gen.Positions) != 2 || gen.Positions[0] != 0 || gen.Positions[1] != 1 {
		t.Fatalf("mid-block generation = %+v, %v", gen, ok)
	}

	seal, _ := c.seal([]byte("header-101"))
	mustApply(t, s, seal)

	gen, ok = s.Generation(101)
	if !ok || !gen.Sealed || len(gen.Positions) != 3 || gen.Positions[2] != 2 {
		t.Fatalf("sealed generation = %+v, %v", gen, ok)
	}

	if _, ok := s.Generation(999); ok {
		t.Error("unknown height reported a generation")
	}
}

func TestGenerationReturnsCopy(t *testing.T) {
	s := New(testChainID)
	c := newChain(t)
	mustApply(t, s, c.open(101, [32]byte{0xef}), c.record([]byte{0x01}))

	gen, _ := s.Generation(101)
	gen.Positions[0] = 7777

	again, _ := s.Generation(101)
	if again.Positions[0] != 0 {
		t.Error("Generation aliases internal storage")
	}
}

func TestSnapshotRestoreMidBlock(t *testing.T) {
	a := New(testChainID)
	c := newChain(t)

	mustApply(t, a, c.open(101, [32]byte{0xef}), c.record([]byte{0x01}))

	seal1, hash1 := c.seal([]byte("header-101"))
	mustApply(t, a, seal1, c.open(102, hash1), c.record([]byte{0x02}))

	snap, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	b := New(1)
	if err := b.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Both states accept the rest of block 102 and agree on everything.
	for _, s := range []*State{a, b} {
		cc := *c
		seal2, hash2 := cc.seal([]byte("header-102"))
		mustApply(t, s, seal2, cc.open(103, hash2))
	}

	if a.Head() != b.Head() {
		t.Errorf("heads diverged after restore: %x vs %x", a.Head(), b.Head())
	}

	if a.Seed() != b.Seed() {
		t.Errorf("seeds diverged after restore: %x vs %x", a.Seed(), b.Seed())
	}

	genA, _ := a.Generation(103)
	genB, okB := b.Generation(103)

	if !okB || genA.Positions[0] != genB.Positions[0] {
		t.Errorf("generations diverged after restore: %+v vs %+v", genA, genB)
	}
}

func TestRestoreKeepsKnownParents(t *testing.T) {
	a := New(testChainID)
	c := newChain(t)
	mustApply(t, a, c.open(101, [32]byte{0xef}), c.record([]byte{0x01}))

	seal1, hash1 := c.seal([]byte("header-101"))
	mustApply(t, a, seal1)

	snap, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	b := New(testChainID)
	if err := b.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// hash1 is a known parent: an open at the wrong height must still be
	// rejected after restore.
	wrong := c.open(200, hash1)
	if _, status := b.Apply(wrong); status != pb.AckStatus_ACK_STATUS_MALFORMED {
		t.Errorf("known-parent height check after restore = %v, want MALFORMED", status)
	}
}

func TestRestoreErrors(t *testing.T) {
	valid := New(testChainID)

	snap, err := valid.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"wrong_version", append([]byte{0x7f}, snap[1:]...)},
		{"corrupt_payload", snap[:len(snap)/2]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(testChainID)
			c := newChain(t)
			mustApply(t, s, c.open(101, [32]byte{0xef}))

			before := s.Head()

			if err := s.Restore(tt.data); err == nil {
				t.Fatal("expected error, got nil")
			}

			if s.Head() != before {
				t.Error("failed Restore mutated the state")
			}
		})
	}
}

func TestApplyRejectionLeavesStateUnchanged(t *testing.T) {
	s := New(testChainID)
	c := newChain(t)
	mustApply(t, s, c.open(101, [32]byte{0xef}))

	before := s.Head()

	stale := newChain(t) // never advanced: stale prefix
	if _, status := s.Apply(stale.open(102, [32]byte{0xaa})); status != pb.AckStatus_ACK_STATUS_STALE_COMMITMENT {
		t.Fatalf("stale Apply = %v, want STALE_COMMITMENT", status)
	}

	malformed := &pb.Entry{Kind: &pb.Entry_Record{Record: &pb.Record{PrefixCommitment: before.Bytes()}}}
	if _, status := s.Apply(malformed); status != pb.AckStatus_ACK_STATUS_MALFORMED {
		t.Fatalf("malformed Apply = %v, want MALFORMED", status)
	}

	if s.Head() != before {
		t.Error("rejected entries advanced the head")
	}

	// The next valid entry lands at the position rejections must not consume.
	mustApply(t, s, c.record([]byte{0x01}))

	gen, _ := s.Generation(101)
	if len(gen.Positions) != 2 || gen.Positions[1] != 1 {
		t.Errorf("rejections consumed positions: %+v", gen)
	}
}
