package raftgrpc

import (
	"github.com/hashicorp/raft"
	"testing"
)

// TestRaftGRPCTransportImplementsRaftTransport checks if the transport implements the raft interface correctly
func TestRaftGRPCTransportImplementsRaftTransport(t *testing.T) {
	var typeAssertion raft.Transport = &RaftGRPCTransport{}
	_ = typeAssertion
}

// TestRaftGRPCTransportImplementsService checks if the transport implements the server interface correctly
func TestRaftGRPCTransportImplementsService(t *testing.T) {
	var typeAssertion RaftServiceServer = (&RaftGRPCTransport{}).GetServerService()
	_ = typeAssertion
}
