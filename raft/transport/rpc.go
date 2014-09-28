package transport

import (
	"bytes"
	"io"
	"runtime/debug"
	"strings"

	"github.com/goraft/raft"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/node/ring"
	"github.com/zond/drafty/raft/commands"
	"github.com/zond/drafty/switchboard"
)

type encoder interface {
	Encode(io.Writer) (int, error)
}

type decoder interface {
	Decode(io.Reader) (int, error)
}

type PeerAdder interface {
	AddPeer(*ring.Peer) *ring.Ring
}

type RPCServer struct {
	Raft      raft.Server
	PeerAdder PeerAdder
}

func (self *RPCServer) setBytes(item encoder, resp *[]byte) (err error) {
	buf := &bytes.Buffer{}
	if _, err = item.Encode(buf); err != nil {
		return
	}
	*resp = buf.Bytes()
	return
}

func (self *RPCServer) RequestVote(req []byte, resp *[]byte) (err error) {
	request := &raft.RequestVoteRequest{}
	if _, err = request.Decode(bytes.NewBuffer(req)); err != nil {
		return
	}
	response := self.Raft.RequestVote(request)
	return self.setBytes(response, resp)
}

func (self *RPCServer) AppendEntries(req []byte, resp *[]byte) (err error) {
	request := &raft.AppendEntriesRequest{}
	if _, err = request.Decode(bytes.NewBuffer(req)); err != nil {
		return
	}
	response := self.Raft.AppendEntries(request)
	if err = self.setBytes(response, resp); err != nil {
		return
	}
	return
}

func (self *RPCServer) RequestSnapshot(req []byte, resp *[]byte) (err error) {
	request := &raft.SnapshotRequest{}
	if _, err = request.Decode(bytes.NewBuffer(req)); err != nil {
		return
	}
	response := self.Raft.RequestSnapshot(request)
	return self.setBytes(response, resp)
}

func (self *RPCServer) SnapshotRecoveryRequest(req []byte, resp *[]byte) (err error) {
	request := &raft.SnapshotRecoveryRequest{}
	if _, err = request.Decode(bytes.NewBuffer(req)); err != nil {
		return
	}
	response := self.Raft.SnapshotRecoveryRequest(request)
	return self.setBytes(response, resp)
}

type JoinRequest struct {
	RaftJoinCommand *raft.DefaultJoinCommand
	Pos             []byte
}

type JoinResponse struct {
	Name string
}

func (self *RPCServer) Join(req *JoinRequest, result *JoinResponse) (err error) {
	if self.Raft.Name() != self.Raft.Leader() {
		return switchboard.Switch.Call(self.Raft.Peers()[self.Raft.Leader()].ConnectionString, "Raft.Join", req, result)
	}
	if _, err = self.Raft.Do(req.RaftJoinCommand); err != nil {
		log.Warnf("Unable to add new node %v: %v", req.RaftJoinCommand.Name, err)
		return
	}
	if _, err = self.Raft.Do(&commands.StopCommand{}); err != nil {
		log.Warnf("Unable to stop cluster after adding new node %v: %v", req.RaftJoinCommand.Name, err)
		return
	}
	if _, err = self.Raft.Do(&commands.ContCommand{
		Ring: self.PeerAdder.AddPeer(&ring.Peer{
			Name:             req.RaftJoinCommand.Name,
			ConnectionString: req.RaftJoinCommand.ConnectionString,
			Pos:              req.Pos,
		}),
	}); err != nil {
		log.Warnf("Unable to restart cluster after adding new node %v: %v", req.RaftJoinCommand.Name, err)
		return
	}
	log.Infof("%v accepted %v", self.Raft.Name(), req.RaftJoinCommand.Name)
	*result = JoinResponse{
		Name: self.Raft.Name(),
	}
	return
}

type PeerRemover interface {
	RemovePeer(string) *ring.Ring
}

type RPCTransport struct {
	Raft        raft.Server
	PeerRemover PeerRemover
}

func (self *RPCTransport) callEncoded(peer *raft.Peer, service string, req encoder, resp decoder) (err error) {
	buf := &bytes.Buffer{}
	if _, err = req.Encode(buf); err != nil {
		log.Debugf("Failed calling %v#%v: %v\n%s", peer.ConnectionString, service, err, debug.Stack())
		return
	}
	b := []byte{}
	if err = switchboard.Switch.Call(peer.ConnectionString, service, buf.Bytes(), &b); err != nil {
		log.Debugf("Failed calling %v#%v: %v\n%s", peer.ConnectionString, service, err, debug.Stack())
		if strings.Contains(err.Error(), "connection refused") {
			if _, err = self.Raft.Do(&raft.DefaultLeaveCommand{
				Name: peer.Name,
			}); err != nil {
				log.Warnf("Unable to kick unreachable node %v: %v", peer.Name, err)
				return
			}
			log.Infof("%v kicked %v from raft", self.Raft.Name(), peer.Name)
			if _, err = self.Raft.Do(&commands.StopCommand{}); err != nil {
				log.Warnf("Unable to stop cluster after kicking unreachable node %v: %v", peer.Name, err)
				return
			}
			if _, err = self.Raft.Do(&commands.ContCommand{
				Ring: self.PeerRemover.RemovePeer(peer.Name),
			}); err != nil {
				log.Warnf("Unable to restart cluster after kicking unreachable node %v: %v", peer.Name, err)
				return
			}
		}
		return
	}
	if _, err = resp.Decode(bytes.NewBuffer(b)); err != nil {
		log.Debugf("Failed calling %v#%v: %v\n%s", peer.ConnectionString, service, err, debug.Stack())
		return
	}
	return
}

func (self *RPCTransport) SendVoteRequest(server raft.Server, peer *raft.Peer, req *raft.RequestVoteRequest) (result *raft.RequestVoteResponse) {
	result = &raft.RequestVoteResponse{}
	if err := self.callEncoded(peer, "Raft.RequestVote", req, result); err != nil {
		return nil
	}
	return result
}

func (self *RPCTransport) SendAppendEntriesRequest(server raft.Server, peer *raft.Peer, req *raft.AppendEntriesRequest) (result *raft.AppendEntriesResponse) {
	result = &raft.AppendEntriesResponse{}
	if err := self.callEncoded(peer, "Raft.AppendEntries", req, result); err != nil {
		return nil
	}
	return result
}

func (self *RPCTransport) SendSnapshotRequest(server raft.Server, peer *raft.Peer, req *raft.SnapshotRequest) (result *raft.SnapshotResponse) {
	result = &raft.SnapshotResponse{}
	if err := self.callEncoded(peer, "Raft.RequestSnapshot", req, result); err != nil {
		return nil
	}
	return result
}

func (self *RPCTransport) SendSnapshotRecoveryRequest(server raft.Server, peer *raft.Peer, req *raft.SnapshotRecoveryRequest) (result *raft.SnapshotRecoveryResponse) {
	result = &raft.SnapshotRecoveryResponse{}
	if err := self.callEncoded(peer, "Raft.SnapshotRecoveryRequest", req, result); err != nil {
		return nil
	}
	return result
}
