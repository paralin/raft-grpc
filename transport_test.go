package raftgrpc

import (
	"github.com/hashicorp/raft"
	"testing"
)

// TestRaftGRPCTransportImplementsRaftTransport tests the raft grpc transport implementation.
func TestRaftGRPCTransportImplementsRaftTransport(t *testing.T) {
	var typeAssertion raft.Transport = NewTransport()
}
func TestRaftGRPCTransportImplementsService(t *testing.T) {
	var typeAssertion RaftServiceServer = NewTransport()
}
