package chainstate

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/0xPolygon/sequence-store-proto/commitment"
)

// snapshotVersion tags the Snapshot wire format; Restore rejects any other
// version. Snapshots are an optimization — a holder can always rebuild by
// replaying its log — so a format change needs detection, not migration.
const snapshotVersion = 1

// snapshotState mirrors State with exported fields for encoding.
type snapshotState struct {
	Seed         commitment.Head
	Head         commitment.Head
	Applied      uint64
	Gens         map[uint64]*Generation
	SealedHeight map[[32]byte]uint64
	OpenHeight   uint64
	OpenActive   bool
}

// Snapshot serializes the full state. The result restores an equivalent
// State via Restore on any implementation of this package version.
func (s *State) Snapshot() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(snapshotVersion)

	err := gob.NewEncoder(&buf).Encode(snapshotState{
		Seed:         s.seed,
		Head:         s.head,
		Applied:      s.applied,
		Gens:         s.gens,
		SealedHeight: s.sealedHeight,
		OpenHeight:   s.openHeight,
		OpenActive:   s.openActive,
	})
	if err != nil {
		return nil, fmt.Errorf("encode chainstate snapshot: %w", err)
	}

	return buf.Bytes(), nil
}

// Restore replaces the state with a snapshot's contents. On error the
// state is unchanged.
func (s *State) Restore(data []byte) error {
	if len(data) == 0 || data[0] != snapshotVersion {
		return fmt.Errorf("chainstate snapshot: unsupported or empty format")
	}

	var snap snapshotState
	if err := gob.NewDecoder(bytes.NewReader(data[1:])).Decode(&snap); err != nil {
		return fmt.Errorf("decode chainstate snapshot: %w", err)
	}

	// gob decodes empty maps as nil; the apply path indexes into both.
	if snap.Gens == nil {
		snap.Gens = map[uint64]*Generation{}
	}

	if snap.SealedHeight == nil {
		snap.SealedHeight = map[[32]byte]uint64{}
	}

	s.seed = snap.Seed
	s.head = snap.Head
	s.applied = snap.Applied
	s.gens = snap.Gens
	s.sealedHeight = snap.SealedHeight
	s.openHeight = snap.OpenHeight
	s.openActive = snap.OpenActive

	return nil
}
