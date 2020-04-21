package raftgrpc

import (
	"github.com/hashicorp/raft"
)

// NewAppendEntriesRequest builds a new AppendEntriesRequest from a raft AppendEntriesRequest object.
func NewAppendEntriesRequest(r *raft.AppendEntriesRequest) *AppendEntriesRequest {
	o := &AppendEntriesRequest{
		Term:              r.Term,
		PrevLogEntry:      r.PrevLogEntry,
		PrevLogTerm:       r.PrevLogTerm,
		LeaderCommitIndex: r.LeaderCommitIndex,
		RaftHeader:        &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
	o.Leader = make([]byte, len(r.Leader))
	copy(o.Leader, r.Leader)
	for _, entry := range r.Entries {
		o.Entries = append(o.Entries, NewLogEntry(entry))
	}

	return o
}

// ToRaft converts the proto back to the raft object.
func (r *AppendEntriesRequest) ToRaft() interface{} {
	o := &raft.AppendEntriesRequest{
		Term:              r.Term,
		PrevLogEntry:      r.PrevLogEntry,
		PrevLogTerm:       r.PrevLogTerm,
		LeaderCommitIndex: r.LeaderCommitIndex,
		RPCHeader:         raft.RPCHeader{ProtocolVersion: raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())},
	}
	o.Leader = make([]byte, len(r.Leader))
	copy(o.Leader, r.Leader)
	for _, entry := range r.Entries {
		o.Entries = append(o.Entries, entry.ToRaft())
	}

	return o
}

// NewLogEntry builds a new LogEntry from a raft.Log object.
func NewLogEntry(l *raft.Log) *LogEntry {
	o := &LogEntry{
		Index: l.Index,
		Term:  l.Term,
		Type:  LogEntryType(l.Type),
	}
	o.Data = make([]byte, len(l.Data))
	copy(o.Data, l.Data)
	o.Extensions = make([]byte, len(l.Extensions))
	copy(o.Extensions, l.Extensions)
	return o
}

// ToRaft converts to the equivalent raft type.
func (r *LogEntry) ToRaft() *raft.Log {
	o := &raft.Log{
		Index: r.Index,
		Term:  r.Term,
		Type:  raft.LogType(r.Type),
	}
	o.Data = make([]byte, len(r.Data))
	copy(o.Data, r.Data)
	o.Extensions = make([]byte, len(r.Extensions))
	copy(o.Extensions, r.Extensions)
	return o
}

// CopyRaft converts to the equivalent raft type.
func (r *LogEntry) CopyRaft(log *raft.Log) {
	log.Index = r.Index
	log.Term = r.Term
	log.Type = raft.LogType(r.Type)
	log.Data = make([]byte, len(r.Data))
	copy(log.Data, r.Data)
	log.Extensions = make([]byte, len(r.Extensions))
	copy(log.Extensions, r.Extensions)
}

// NewAppendEntriesResponse builds a new AppendEntriesResponse from a raft AppendEntriesResponse object.
func NewAppendEntriesResponse(r *raft.AppendEntriesResponse) *AppendEntriesResponse {
	return &AppendEntriesResponse{
		Term:           r.Term,
		LastLog:        r.LastLog,
		Success:        r.Success,
		NoRetryBackoff: r.NoRetryBackoff,
		RaftHeader:     &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
}

// CopyToRaft copies to the equivalent raft type.
func (r *AppendEntriesResponse) CopyToRaft(o *raft.AppendEntriesResponse) {
	o.Term = r.Term
	o.LastLog = r.LastLog
	o.Success = r.Success
	o.NoRetryBackoff = r.NoRetryBackoff
	o.RPCHeader.ProtocolVersion = raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())
}

// NewRequestVoteRequest builds the RequestVoteRequest message from the equivalent raft type.
func NewRequestVoteRequest(r *raft.RequestVoteRequest) *RequestVoteRequest {
	o := &RequestVoteRequest{
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		LeadershipTransfer: r.LeadershipTransfer,
		RaftHeader:         &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
	o.Candidate = make([]byte, len(r.Candidate))
	copy(o.Candidate, r.Candidate)
	return o
}

// ToRaft converts to the equivalent raft type.
func (r *RequestVoteRequest) ToRaft() interface{} {
	o := &raft.RequestVoteRequest{
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		LeadershipTransfer: r.LeadershipTransfer,
		RPCHeader:          raft.RPCHeader{ProtocolVersion: raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())},
	}
	o.Candidate = make([]byte, len(r.Candidate))
	copy(o.Candidate, r.Candidate)
	return o
}

// NewRequestVoteResponse builds the RequestVoteResponse message from the equivalent raft type.
func NewRequestVoteResponse(r *raft.RequestVoteResponse) *RequestVoteResponse {
	o := &RequestVoteResponse{
		Term:       r.Term,
		Granted:    r.Granted,
		RaftHeader: &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
	o.Peers = make([]byte, len(r.Peers))
	copy(o.Peers, r.Peers)
	return o
}

// ToRaft converts to the equivalent raft type.
func (r *RequestVoteResponse) CopyToRaft(o *raft.RequestVoteResponse) {
	o.Term = r.Term
	o.Granted = r.Granted
	o.Peers = make([]byte, len(r.Peers))
	copy(o.Peers, r.Peers)
	o.RPCHeader.ProtocolVersion = raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())
}

// NewInstallSnapshotRequest builds a new InstallSnapshotRequest from the raft type.
func NewInstallSnapshotRequest(r *raft.InstallSnapshotRequest) (*InstallSnapshotRequest, error) {
	o := &InstallSnapshotRequest{
		Term:         r.Term,
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
		Size:         r.Size,
		ConfigIdx:    r.ConfigurationIndex,
		RaftHeader:   &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
	o.Leader = make([]byte, len(r.Leader))
	copy(o.Leader, r.Leader)
	o.Peers = make([]byte, len(r.Peers))
	copy(o.Peers, r.Peers)
	o.Configuration = make([]byte, len(r.Configuration))
	copy(o.Configuration, r.Configuration)
	return o, nil
}

// ToRaft converts the message to an equivalent raft type.
func (r *InstallSnapshotRequest) ToRaft() interface{} {
	o := &raft.InstallSnapshotRequest{
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		Size:               r.Size,
		ConfigurationIndex: r.ConfigIdx,
		RPCHeader:          raft.RPCHeader{ProtocolVersion: raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())},
	}
	o.Leader = make([]byte, len(r.Leader))
	copy(o.Leader, r.Leader)
	o.Peers = make([]byte, len(r.Peers))
	copy(o.Peers, r.Peers)
	o.Configuration = make([]byte, len(r.Configuration))
	copy(o.Configuration, r.Configuration)
	return o
}

// NewInstallSnapshotResponse builds an InstallSnapshotResponse from the equivalent raft type.
func NewInstallSnapshotResponse(r *raft.InstallSnapshotResponse) *InstallSnapshotResponse {
	return &InstallSnapshotResponse{
		Term:       r.Term,
		Success:    r.Success,
		RaftHeader: &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
}

// ToRaft converts the message to the equivalent raft type.
func (r *InstallSnapshotResponse) CopyToRaft(resp *raft.InstallSnapshotResponse) {
	resp.Term = r.Term
	resp.Success = r.Success
	resp.RPCHeader.ProtocolVersion = raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())
}

// NewTimeoutNowRequest builds a TimeoutNowRequest struct from the equivalent raft type.
func NewTimeoutNowRequest(r *raft.TimeoutNowRequest) *TimeoutNowRequest {
	return &TimeoutNowRequest{
		RaftHeader: &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
}

// ToRaft converts the message to an equivalent raft type.
func (r *TimeoutNowRequest) ToRaft() interface{} {
	return &raft.TimeoutNowRequest{
		RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())},
	}
}

// NewTimeoutNowResponse builds a TimeoutNowResponse struct from the equivalent raft type.
func NewTimeoutNowResponse(r *raft.TimeoutNowResponse) *TimeoutNowResponse {
	return &TimeoutNowResponse{
		RaftHeader: &RaftHeader{ProtocolVersion: int32(r.ProtocolVersion)},
	}
}

// ToRaft converts the message to the equivalent raft type.
func (r *TimeoutNowResponse) CopyToRaft(resp *raft.TimeoutNowResponse) {
	resp.RPCHeader.ProtocolVersion = raft.ProtocolVersion(r.RaftHeader.GetProtocolVersion())
}
