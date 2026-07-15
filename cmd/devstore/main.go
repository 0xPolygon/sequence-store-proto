// Command devstore serves the in-memory reference sequence store over
// plaintext gRPC — for tests and devnets only.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/0xPolygon/sequence-store-proto/devstore"
	pb "github.com/0xPolygon/sequence-store-proto/sequencestore/v1"
)

func main() {
	addr := flag.String("addr", ":7788", "listen address")
	chainID := flag.Uint64("chain-id", 137, "chain id seeding the commitment chain")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	store := devstore.New(*chainID)

	// Keepalive reaps connections that die without a FIN (NAT timeouts),
	// unparking Stream RPCs idling on their context; the enforcement policy
	// lets clients of those infinite streams ping the server just as often.
	srv := grpc.NewServer(
		// Whole-block Records batched under load outgrow gRPC's 4 MiB
		// default; consumers dial with a matching grpc.MaxCallRecvMsgSize.
		grpc.MaxRecvMsgSize(16<<20),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterPublisherServiceServer(srv, store)
	pb.RegisterConsumerServiceServer(srv, store)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		// Stop, not GracefulStop: consumer streams are infinite, so a
		// graceful drain never completes, and an in-memory store has
		// nothing to flush.
		srv.Stop()
	}()

	log.Printf("devstore listening on %s (chain id %d, seed %x)", *addr, *chainID, store.Head())

	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
