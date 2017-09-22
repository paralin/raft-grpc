# Raft GRPC

[![GoDoc Widget]][GoDoc] [![Go Report Card Widget]][Go Report Card]

[GoDoc]: https://godoc.org/github.com/paralin/raft-grpc
[GoDoc Widget]: https://godoc.org/github.com/paralin/raft-grpc?status.svg
[Go Report Card Widget]: https://goreportcard.com/badge/github.com/paralin/raft-grpc
[Go Report Card]: https://goreportcard.com/report/github.com/paralin/raft-grpc

## Introduction

**raft-grpc** includes a GRPC service definition and implementation of a server and client for pipelining [raft](https://github.com/hashicorp/raft) streams over GRPC.

To use it in your project, include the client or server package and add the service to your GRPC server.

