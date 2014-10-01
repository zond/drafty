package messages

import "github.com/goraft/raft"

type JoinRequest struct {
	RaftJoinCommand *raft.DefaultJoinCommand
	Pos             []byte
}

type JoinResponse struct {
	Name string
}
