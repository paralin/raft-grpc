# Raft GRPC

This project is **no longer maintained**. You might be interested in https://github.com/Jille/raft-grpc-transport instead.

[![GoDoc Widget]][GoDoc] [![Go Report Card Widget]][Go Report Card]

[GoDoc]: https://godoc.org/github.com/paralin/raft-grpc
[GoDoc Widget]: https://godoc.org/github.com/paralin/raft-grpc?status.svg
[Go Report Card Widget]: https://goreportcard.com/badge/github.com/paralin/raft-grpc
[Go Report Card]: https://goreportcard.com/report/github.com/paralin/raft-grpc

## Introduction

**raft-grpc** includes a GRPC service definition and implementation of a server and client for pipelining [raft](https://github.com/hashicorp/raft) streams over GRPC.

To use it in your project, include the package and add the service to your GRPC server. The RaftGRPCTransport can be used as a raft.Transport object, and the GetServerService call will return the GRPC service implementation for your listener side.

