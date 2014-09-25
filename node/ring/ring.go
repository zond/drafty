package ring

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sort"
)

type Peer struct {
	Name             string
	Pos              []byte
	ConnectionString string
}

func (self *Peer) String() string {
	return fmt.Sprintf("Peer{Addr:%v, Name:%v, Pos:%v}", self.ConnectionString, self.Name, hex.EncodeToString(self.Pos))
}

type Peers []*Peer

func (self Peers) Len() int {
	return len(self)
}

func (self Peers) Less(i, j int) bool {
	return bytes.Compare(self[i].Pos, self[j].Pos) < 0
}

func (self Peers) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type Ring struct {
	peers      Peers
	peerByName map[string]*Peer
}

func New() *Ring {
	return &Ring{
		peerByName: map[string]*Peer{},
	}
}

type gobRing struct {
	Peers      Peers
	PeerByName map[string]*Peer
}

func (self *Ring) MarshalBinary() (b []byte, err error) {
	buf := &bytes.Buffer{}
	if err = gob.NewEncoder(buf).Encode(gobRing{
		Peers:      self.peers,
		PeerByName: self.peerByName,
	}); err != nil {
		return
	}
	b = buf.Bytes()
	return
}

func (self *Ring) UnmarshalBinary(b []byte) (err error) {
	g := &gobRing{}
	if err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(g); err != nil {
		return
	}
	self.peers, self.peerByName = g.Peers, g.PeerByName
	return
}

func (self *Ring) AddPeer(peer *Peer) {
	self.peers = append(self.peers, peer)
	self.peerByName[peer.Name] = peer
	sort.Sort(self.peers)
}

func (self *Ring) ByName(name string) (result *Peer, found bool) {
	result, found = self.peerByName[name]
	return
}

func (self *Ring) RemovePeer(name string) {
	peer, found := self.peerByName[name]
	if !found {
		return
	}
	delete(self.peerByName, name)
	i := sort.Search(len(self.peers), func(p int) bool {
		return bytes.Compare(self.peers[p].Pos, peer.Pos) > -1
	})
	if i == 0 {
		self.peers = self.peers[1:]
	} else if i == len(self.peers) {
		self.peers = self.peers[:len(self.peers)-1]
	} else {
		self.peers = append(self.peers[:i], self.peers[i+1:]...)
	}
	return
}

func (self *Ring) Successors(pos []byte, n int) (result Peers) {
	if len(self.peers) == 0 {
		return
	}
	result = make(Peers, 0, n)
	for i := sort.Search(len(self.peers), func(p int) bool {
		return bytes.Compare(self.peers[p].Pos, pos) > 0
	}); len(result) < n; i++ {
		result = append(result, self.peers[i%len(self.peers)])
	}
	return
}
