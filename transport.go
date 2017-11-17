package raftgrpc

import (
	"bytes"
	"golang.org/x/net/context"
	"io"
	"sync"
	"time"

	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

var rpcMaxPipeline int = 20

// RaftGRPCTransport implements raft.Transport over GRPC.
type RaftGRPCTransport struct {
	ctx context.Context

	peers    map[raft.ServerAddress]RaftServiceClient
	peersMtx sync.RWMutex

	rpcCh        chan raft.RPC
	localAddress raft.ServerAddress

	heartbeatHandlerMtx sync.RWMutex
	heartbeatHandler    func(rpc raft.RPC)
}

// raftGrpcTransportServer is the server wrapping the transport object
type raftGrpcTransportServer struct {
	*RaftGRPCTransport
}

// NewTransport builds a new transport service.
func NewTransport(ctx context.Context, localAddress raft.ServerAddress) *RaftGRPCTransport {
	return &RaftGRPCTransport{
		ctx:          ctx,
		localAddress: localAddress,
		peers:        make(map[raft.ServerAddress]RaftServiceClient),
		rpcCh:        make(chan raft.RPC),
	}
}

// getPeerClient looks up a peer client.
func (t *RaftGRPCTransport) getPeerClient(target raft.ServerAddress) (RaftServiceClient, error) {
	t.peersMtx.RLock()
	defer t.peersMtx.RUnlock()

	client, ok := t.peers[target]
	if !ok {
		return nil, fmt.Errorf("no connection to peer is available: %s", target)
	}
	return client, nil
}

// Consumer returns a channel that raft uses to process incoming requests.
func (t *RaftGRPCTransport) Consumer() <-chan raft.RPC {
	return t.rpcCh
}

// LocalAddr returns the local address to distinguish from peers.
func (t *RaftGRPCTransport) LocalAddr() raft.ServerAddress {
	return t.localAddress
}

// callAppendPipeline implements raft.AppendPipeline for a running GRPC streaming call.
type callAppendPipeline struct {
	ctx        context.Context
	ctxCancel  context.CancelFunc
	client     RaftService_AppendEntriesPipelineClient
	pipelineCh chan *callAppendPipelineInFlight
	doneCh     chan raft.AppendFuture
}

// processPipeline manages the pipeline.
func (p *callAppendPipeline) processPipeline() (retErr error) {
	respCh := make(chan *callAppendPipelineInFlight, rpcMaxPipeline)
	defer func() {
		// drain the resp queue
		p.Close()
		close(respCh)
		for call := range respCh {
			call.respCh <- retErr
		}
	}()

	go func() {
		defer p.Close()
		for {
			msg, err := p.client.Recv()
			if err != nil {
				return
			}
			select {
			case <-p.ctx.Done():
				return
			case r := <-respCh:
				if msg.Error != "" {
					err := errors.New(msg.Error)
					r.respCh <- err
					return
				}

				msg.Response.CopyToRaft(r.resp)
				r.respCh <- nil
				p.doneCh <- r
			}
		}
	}()

	for {
		select {
		case <-p.ctx.Done():
			return context.Canceled
		case call := <-p.pipelineCh:
			select {
			case respCh <- call:
			case <-p.ctx.Done():
				call.respCh <- context.Canceled
				return context.Canceled
			}
			if err := p.client.Send(call.args); err != nil {
				return err
			}
		}
	}
}

// callAppendPipelineInFlight is an in-flight request.
type callAppendPipelineInFlight struct {
	ctx       context.Context
	startTime time.Time
	args      *AppendEntriesRequest
	resp      *raft.AppendEntriesResponse
	respCh    chan error
}

// Error returns when the call finishes.
func (c *callAppendPipelineInFlight) Error() error {
	select {
	case <-c.ctx.Done():
		return context.Canceled
	case err := <-c.respCh:
		return err
	}
}

// Start returns the start time.
func (c *callAppendPipelineInFlight) Start() time.Time {
	return c.startTime
}

// Request holds the parameters of the AppendEntries call.
// It is always OK to call this method.
func (c *callAppendPipelineInFlight) Request() *raft.AppendEntriesRequest {
	return c.args.ToRaft().(*raft.AppendEntriesRequest)
}

// Response holds the results of the AppendEntries call.
// This method must only be called after the Error
// method returns, and will only be valid on success.
func (c *callAppendPipelineInFlight) Response() *raft.AppendEntriesResponse {
	return c.resp
}

// AppendEntries is used to add another request to the pipeline.
// The send may block which is an effective form of back-pressure.
func (p *callAppendPipeline) AppendEntries(
	args *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse,
) (raft.AppendFuture, error) {
	respCh := make(chan error, 1)
	call := &callAppendPipelineInFlight{
		ctx:       p.ctx,
		startTime: time.Now(),
		args:      NewAppendEntriesRequest(args),
		resp:      resp,
		respCh:    respCh,
	}
	select {
	case <-p.ctx.Done():
		return nil, context.Canceled
	case p.pipelineCh <- call:
	}
	return call, nil
}

// Consumer returns a channel that can be used to consume
// response futures when they are ready.
func (p *callAppendPipeline) Consumer() <-chan raft.AppendFuture {
	return p.doneCh
}

// Close closes the pipeline and cancels all inflight RPCs
func (p *callAppendPipeline) Close() error {
	p.ctxCancel()
	return nil
}

// AppendEntriesPipeline returns an interface that can be used to pipeline AppendEntries requests.
func (t *RaftGRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	conn, err := t.getPeerClient(target)
	if err != nil {
		return nil, err
	}

	pipe := &callAppendPipeline{
		doneCh:     make(chan raft.AppendFuture, rpcMaxPipeline),
		pipelineCh: make(chan *callAppendPipelineInFlight, rpcMaxPipeline),
	}
	pipe.ctx, pipe.ctxCancel = context.WithCancel(t.ctx)
	pipeClient, err := conn.AppendEntriesPipeline(pipe.ctx)
	if err != nil {
		return nil, err
	}
	pipe.client = pipeClient
	pipe.ctx = pipeClient.Context()
	go pipe.processPipeline()

	return pipe, nil
}

// AppendEntries sends the appropriate RPC to the target node.
func (t *RaftGRPCTransport) AppendEntries(
	id raft.ServerID,
	target raft.ServerAddress,
	args *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse,
) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	res, err := client.AppendEntries(
		t.ctx,
		NewAppendEntriesRequest(args),
	)
	if err != nil {
		return err
	}
	res.CopyToRaft(resp)
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (t *RaftGRPCTransport) RequestVote(
	id raft.ServerID,
	target raft.ServerAddress,
	args *raft.RequestVoteRequest,
	resp *raft.RequestVoteResponse,
) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	res, err := client.RequestVote(
		t.ctx,
		NewRequestVoteRequest(args),
	)
	if err != nil {
		return err
	}
	res.CopyToRaft(resp)
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (t *RaftGRPCTransport) InstallSnapshot(
	id raft.ServerID,
	target raft.ServerAddress,
	args *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse,
	data io.Reader,
) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	snp, err := NewInstallSnapshotRequest(args, data)
	if err != nil {
		return err
	}

	res, err := client.InstallSnapshot(
		t.ctx,
		snp,
	)
	if err != nil {
		return err
	}
	res.CopyToRaft(resp)

	return nil
}

// EncodePeer is used to serialize a peer name.
func (*RaftGRPCTransport) EncodePeer(id raft.ServerID, peerName raft.ServerAddress) []byte {
	dat, _ := proto.Marshal(&PeerNameContainer{PeerName: string(peerName)})
	return dat
}

// DecodePeer is used to deserialize a peer name.
func (*RaftGRPCTransport) DecodePeer(dat []byte) raft.ServerAddress {
	ctr := &PeerNameContainer{}
	_ = proto.Unmarshal(dat, ctr)
	return raft.ServerAddress(ctr.PeerName)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (t *RaftGRPCTransport) SetHeartbeatHandler(
	cb func(rpc raft.RPC),
) {
	t.heartbeatHandlerMtx.Lock()
	t.heartbeatHandler = cb
	t.heartbeatHandlerMtx.Unlock()
}

// GetServerService returns a wrapper that contains the server methods.
func (t *RaftGRPCTransport) GetServerService() RaftServiceServer {
	return &raftGrpcTransportServer{RaftGRPCTransport: t}
}

// AppendEntriesPipeline receives an AppendEntries message stream open request.
func (t *raftGrpcTransportServer) AppendEntriesPipeline(
	s RaftService_AppendEntriesPipelineServer,
) error {

	for {
		call, err := s.Recv()
		if err != nil {
			return err
		}

		respCh := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			Command:  call.ToRaft(),
			RespChan: respCh,
		}

		isHeartbeat := call.Term != 0 && call.Leader != nil &&
			call.PrevLogEntry == 0 && call.PrevLogTerm == 0 &&
			len(call.Entries) == 0 && call.LeaderCommitIndex == 0
		if isHeartbeat {
			var hh func(r raft.RPC)
			t.heartbeatHandlerMtx.RLock()
			if t.heartbeatHandler != nil {
				hh = t.heartbeatHandler
			}
			t.heartbeatHandlerMtx.RUnlock()
			if hh != nil {
				hh(rpc)
				return nil
			}
		}

		select {
		case t.rpcCh <- rpc:
		case <-t.ctx.Done():
			return context.Canceled
		}

		go func(respCh <-chan raft.RPCResponse) {
			select {
			case <-t.ctx.Done():
				return
			case res := <-respCh:
				r := &AppendEntriesPipelineResponse{}
				if res.Error != nil {
					r.Error = res.Error.Error()
				} else {
					r.Response = NewAppendEntriesResponse(
						res.Response.(*raft.AppendEntriesResponse),
					)
				}
				if err := s.Send(r); err != nil {
					return
				}
			}
		}(respCh)
	}
}

type toRaftable interface {
	ToRaft() interface{}
}

func (t *raftGrpcTransportServer) processRpc(req toRaftable, rdr io.Reader) (interface{}, error) {
	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  req.ToRaft(),
		RespChan: respCh,
		Reader:   rdr,
	}

	select {
	case <-t.ctx.Done():
		return nil, context.Canceled
	case t.rpcCh <- rpc:
	}

	select {
	case <-t.ctx.Done():
		return nil, context.Canceled
	case res := <-respCh:
		return res.Response, res.Error
	}
}

// AppendEntries performs a single append entries request / response.
func (t *raftGrpcTransportServer) AppendEntries(
	ctx context.Context,
	req *AppendEntriesRequest,
) (*AppendEntriesResponse, error) {
	ri, err := t.processRpc(req, nil)

	if err != nil {
		return nil, err
	}
	return NewAppendEntriesResponse(
		ri.(*raft.AppendEntriesResponse),
	), nil
}

// RequestVote is the command used by a candidate to ask a Raft peer for a vote in an election.
func (t *raftGrpcTransportServer) RequestVote(
	ctx context.Context,
	req *RequestVoteRequest,
) (*RequestVoteResponse, error) {
	ri, err := t.processRpc(req, nil)

	if err != nil {
		return nil, err
	}
	return NewRequestVoteResponse(
		ri.(*raft.RequestVoteResponse),
	), nil
}

// InstallSnapshot is the command sent to a Raft peer to bootstrap its log (and state machine) from a snapshot on another peer.
func (t *raftGrpcTransportServer) InstallSnapshot(
	ctx context.Context,
	req *InstallSnapshotRequest,
) (*InstallSnapshotResponse, error) {
	ri, err := t.processRpc(
		req,
		bytes.NewReader(req.GetSnapshot()),
	)

	if err != nil {
		return nil, err
	}
	return NewInstallSnapshotResponse(
		ri.(*raft.InstallSnapshotResponse),
	), nil
}

// AddPeer adds a peer by id to the transport.
func (t *RaftGRPCTransport) AddPeer(id raft.ServerID, target raft.ServerAddress, peerConn RaftServiceClient) {
	t.peersMtx.Lock()
	t.peers[target] = peerConn
	t.peersMtx.Unlock()
}

// RemovePeer removes a peer by id from the transport.
func (t *RaftGRPCTransport) RemovePeer(id raft.ServerID, target raft.ServerAddress) {
	t.peersMtx.Lock()
	delete(t.peers, target)
	t.peersMtx.Unlock()
}
