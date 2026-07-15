package devstore

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/0xPolygon/sequence-store-proto/commitment"
	pb "github.com/0xPolygon/sequence-store-proto/sequencestore/v1"
)

func TestResumePositions(t *testing.T) {
	store, _, con := setupGRPC(t)
	_, _, _ = seedBlocks(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// after=head of the second entry resumes at the third.
	store.mu.Lock()
	secondHead := store.heads[1]
	total := len(store.log)
	store.mu.Unlock()

	resp, err := con.Range(ctx, &pb.RangeRequest{
		After: &pb.RangeRequest_Head{Head: secondHead[:]},
	})
	if err != nil {
		t.Fatalf("Range after=head: %v", err)
	}

	if got, want := len(resp.GetEntries()), total-2; got != want {
		t.Errorf("after=head returned %d entries, want %d", got, want)
	}

	if !resp.GetLive() {
		t.Error("expected live=true at tip")
	}

	// after=block:101 resumes after block 101's seal (3 entries in).
	resp, err = con.Range(ctx, &pb.RangeRequest{
		After: &pb.RangeRequest_Block{Block: 101},
	})
	if err != nil {
		t.Fatalf("Range after=block: %v", err)
	}

	if got, want := len(resp.GetEntries()), total-3; got != want {
		t.Errorf("after=block returned %d entries, want %d", got, want)
	}

	// Unknown head and unknown block are NOT_FOUND; a short head is
	// INVALID_ARGUMENT.
	for name, req := range map[string]*pb.RangeRequest{
		"unknown_head":  {After: &pb.RangeRequest_Head{Head: make([]byte, 32)}},
		"unknown_block": {After: &pb.RangeRequest_Block{Block: 999}},
	} {
		if _, err := con.Range(ctx, req); status.Code(err) != codes.NotFound {
			t.Errorf("%s: code = %v, want NotFound", name, status.Code(err))
		}
	}

	_, err = con.Range(ctx, &pb.RangeRequest{After: &pb.RangeRequest_Head{Head: []byte{0x01}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("short head: code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestRangePaging(t *testing.T) {
	store, _, con := setupGRPC(t)
	_, _, _ = seedBlocks(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		got  int
		next []byte
	)

	for {
		req := &pb.RangeRequest{Limit: 2}
		if next != nil {
			req.After = &pb.RangeRequest_Head{Head: next}
		}

		resp, err := con.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}

		if len(resp.GetEntries()) > 2 {
			t.Fatalf("page has %d entries, limit is 2", len(resp.GetEntries()))
		}

		got += len(resp.GetEntries())
		next = resp.GetNext()

		if resp.GetLive() {
			break
		}
	}

	store.mu.Lock()
	total := len(store.log)
	store.mu.Unlock()
	head := store.Head()

	if got != total {
		t.Errorf("paged %d entries, want %d", got, total)
	}

	if commitment.Head(next) != head {
		t.Errorf("final next = %x, want store head %x", next, head)
	}

	// An empty response at the tip echoes the presented position.
	resp, err := con.Range(ctx, &pb.RangeRequest{After: &pb.RangeRequest_Head{Head: next}})
	if err != nil {
		t.Fatalf("Range at tip: %v", err)
	}

	if len(resp.GetEntries()) != 0 || commitment.Head(resp.GetNext()) != head {
		t.Errorf("tip range = %d entries, next %x; want 0 entries, next %x",
			len(resp.GetEntries()), resp.GetNext(), head)
	}
}

// Byte-budgeted paging: every page stays under gRPC's default 4 MiB recv
// size and next resumes past what was delivered.
func TestRangeByteBudget(t *testing.T) {
	store, _, con := setupGRPC(t)

	c := newChain(t)
	mustAppend(t, store, c.open(101, [32]byte{0xef}))

	tx := make([]byte, 1<<20)
	for range 4 {
		mustAppend(t, store, c.record(tx))
	}

	seal, _ := c.seal([]byte("header-101"))
	mustAppend(t, store, seal)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		got  int
		next []byte
	)

	for {
		req := &pb.RangeRequest{}
		if next != nil {
			req.After = &pb.RangeRequest_Head{Head: next}
		}

		resp, err := con.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range: %v", err)
		}

		if size := proto.Size(resp); size > 4<<20 {
			t.Fatalf("page is %d bytes, over the 4 MiB default recv size", size)
		}

		if len(resp.GetEntries()) == 0 && !resp.GetLive() {
			t.Fatal("empty non-live page: no progress")
		}

		got += len(resp.GetEntries())
		next = resp.GetNext()

		if resp.GetLive() {
			break
		}
	}

	if got != 6 {
		t.Errorf("paged %d entries, want 6", got)
	}
}

func TestBoundBytesOversizedEntry(t *testing.T) {
	entry := &pb.Entry{Kind: &pb.Entry_Record{Record: &pb.Record{
		Transactions:     [][]byte{make([]byte, 2*maxRangeBytes)},
		PrefixCommitment: make([]byte, 32),
	}}}

	if kept, _ := boundBytes([]*pb.Entry{entry}, maxRangeBytes, true); len(kept) != 1 {
		t.Errorf("leading oversized entry: kept %d, want 1", len(kept))
	}

	if kept, _ := boundBytes([]*pb.Entry{entry}, maxRangeBytes, false); len(kept) != 0 {
		t.Errorf("non-leading oversized entry: kept %d, want 0", len(kept))
	}
}

// An unpositioned Range on an empty store returns the seed as next — the
// chain-start position, valid to page from.
func TestRangeEmptyStore(t *testing.T) {
	store, _, con := setupGRPC(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := con.Range(ctx, &pb.RangeRequest{})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(resp.GetEntries()) != 0 || !resp.GetLive() {
		t.Errorf("empty store range = %d entries, live %v; want 0, true",
			len(resp.GetEntries()), resp.GetLive())
	}

	if commitment.Head(resp.GetNext()) != store.Head() {
		t.Errorf("next = %x, want seed %x", resp.GetNext(), store.Head())
	}
}

func TestRangeLongPoll(t *testing.T) {
	store, _, con := setupGRPC(t)
	c, _, hash2 := seedBlocks(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	head := store.Head()

	done := make(chan *pb.RangeResponse, 1)

	go func() {
		resp, err := con.Range(ctx, &pb.RangeRequest{
			After:  &pb.RangeRequest_Head{Head: head[:]},
			WaitMs: 5000,
		})
		if err != nil {
			t.Errorf("long-poll Range: %v", err)
		}

		done <- resp
	}()

	time.Sleep(50 * time.Millisecond)
	mustAppend(t, store, c.open(103, hash2))

	select {
	case resp := <-done:
		if len(resp.GetEntries()) != 1 {
			t.Errorf("long-poll returned %d entries, want 1", len(resp.GetEntries()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll did not return after append")
	}
}

func TestGetBlockUnknown(t *testing.T) {
	_, _, con := setupGRPC(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := con.GetBlock(ctx, &pb.GetBlockRequest{BlockNumber: 1})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

// After the Live frame, appends keep flowing as envelopes and Live is never
// repeated.
func TestStreamFollowsAfterLive(t *testing.T) {
	store, _, con := setupGRPC(t)
	c, _, hash2 := seedBlocks(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	head := store.Head()

	cs, err := con.Stream(ctx, &pb.StreamRequest{After: &pb.StreamRequest_Head{Head: head.Bytes()}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	frame, err := cs.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}

	if frame.GetLive() == nil {
		t.Fatal("expected Live first when starting at the tip")
	}

	mustAppend(t, store, c.open(103, hash2))
	mustAppend(t, store, c.record([]byte{0x05}))

	for i := range 2 {
		frame, err := cs.Recv()
		if err != nil {
			t.Fatalf("recv live entry %d: %v", i, err)
		}

		if frame.GetEntry() == nil {
			t.Fatalf("frame %d after Live is not an entry (duplicate Live?)", i)
		}
	}

	// Live marks one transition, not idleness: let the server drain to an
	// empty tail, then append again — the next frame must be the entry, not
	// a second Live.
	time.Sleep(200 * time.Millisecond)
	mustAppend(t, store, c.record([]byte{0x06}))

	frame, err = cs.Recv()
	if err != nil {
		t.Fatalf("recv after idle: %v", err)
	}

	if frame.GetEntry() == nil {
		t.Fatal("frame after idle gap is not an entry (duplicate Live?)")
	}
}

func TestStreamAfterBlock(t *testing.T) {
	store, _, con := setupGRPC(t)
	_, _, _ = seedBlocks(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cs, err := con.Stream(ctx, &pb.StreamRequest{After: &pb.StreamRequest_Block{Block: 101}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// Block 102's three entries, then Live.
	for i := range 3 {
		frame, err := cs.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}

		if frame.GetEntry() == nil {
			t.Fatalf("frame %d is not an entry", i)
		}
	}

	frame, err := cs.Recv()
	if err != nil {
		t.Fatalf("live: %v", err)
	}

	if frame.GetLive() == nil {
		t.Fatal("expected Live after block 102's entries")
	}
}

// A block-addressed resume of a still-open generation serves it from its
// open record, never from the middle.
func TestResumeUnsealedGeneration(t *testing.T) {
	store, _, con := setupGRPC(t)
	c, _, hash2 := seedBlocks(t, store)

	mustAppend(t, store, c.open(103, hash2))
	mustAppend(t, store, c.record([]byte{0x06}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := con.Range(ctx, &pb.RangeRequest{After: &pb.RangeRequest_Block{Block: 103}})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if got := len(resp.GetEntries()); got != 2 {
		t.Fatalf("unsealed resume returned %d entries, want 2", got)
	}

	open := resp.GetEntries()[0].GetBlockOpen()
	if open.GetBlockNumber() != 103 {
		t.Errorf("first entry is not block 103's open (number %d)", open.GetBlockNumber())
	}
}
