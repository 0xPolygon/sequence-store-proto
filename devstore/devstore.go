// Package devstore is an in-memory reference implementation of the sequence
// store for tests and devnets: the shared chainstate state machine composed
// with an in-memory log — the full wire contract with no persistence, no
// authentication, and no replication. Not for production.
package devstore

import (
	"errors"
	"io"
	"sync"

	"github.com/0xPolygon/sequence-store-proto/chainstate"
	"github.com/0xPolygon/sequence-store-proto/commitment"
	pb "github.com/0xPolygon/sequence-store-proto/sequencestore/v1"
)

// Store holds one append-only chain of entries guarded by the head check.
// It implements both generated gRPC service interfaces.
type Store struct {
	pb.UnimplementedPublisherServiceServer
	pb.UnimplementedConsumerServiceServer

	mu    sync.Mutex
	state *chainstate.State
	log   []*pb.Entry
	heads []commitment.Head // post-fold head per log position

	// resume maps a post-fold head to the position right after its entry;
	// the seed maps to 0.
	resume map[commitment.Head]int

	// notify is closed and replaced on every append; readers wait on it.
	notify chan struct{}
}

// New returns an empty store whose head is the genesis seed for chainID.
func New(chainID uint64) *Store {
	state := chainstate.New(chainID)

	return &Store{
		state:  state,
		resume: map[commitment.Head]int{state.Seed(): 0},
		notify: make(chan struct{}),
	}
}

// Head returns the store's current commitment head.
func (s *Store) Head() commitment.Head {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state.Head()
}

// Append runs the ingress path for one entry through the shared state
// machine, appending accepted entries to the in-memory log. The store takes
// ownership of an accepted entry — mutating it afterwards corrupts the log
// for every reader.
func (s *Store) Append(entry *pb.Entry) pb.AckStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	next, status := s.state.Apply(entry)
	if status != pb.AckStatus_ACK_STATUS_OK {
		return status
	}

	pos := len(s.log)
	s.log = append(s.log, entry)
	s.heads = append(s.heads, next)
	s.resume[next] = pos + 1

	close(s.notify)
	s.notify = make(chan struct{})

	return pb.AckStatus_ACK_STATUS_OK
}

// Publish implements the pipelined write path: one ack per entry, in order.
func (s *Store) Publish(stream pb.PublisherService_PublishServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		status := s.Append(req.GetEntry())
		if err := stream.Send(&pb.PublishResponse{Status: status}); err != nil {
			return err
		}
	}
}
