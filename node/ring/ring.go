package ring

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
)

func RandomPos(octets int) (result []byte) {
	result = make([]byte, 8*octets)
	for n := 0; n < octets; n++ {
		i := rand.Int63()
		result[0+n*8] = byte(i >> 7)
		result[1+n*8] = byte(i >> 6)
		result[2+n*8] = byte(i >> 5)
		result[3+n*8] = byte(i >> 4)
		result[4+n*8] = byte(i >> 3)
		result[5+n*8] = byte(i >> 2)
		result[6+n*8] = byte(i >> 1)
		result[7+n*8] = byte(i >> 0)
	}
	return
}

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

func (self *Ring) Clone() (result *Ring) {
	result = &Ring{
		peers:      make(Peers, len(self.peers)),
		peerByName: make(map[string]*Peer, len(self.peers)),
	}
	for i, peer := range self.peers {
		result.peers[i] = peer
		result.peerByName[peer.Name] = peer
	}
	return
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

func (self *Ring) Rand() (result *Peer) {
	return self.peers[rand.Int()%len(self.peers)]
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

func (self *Ring) Predecessors(pos []byte, n int) (result Peers) {
	if len(self.peers) == 0 {
		return
	}
	result = make(Peers, 0, n)
	for i := sort.Search(len(self.peers), func(p int) bool {
		return bytes.Compare(self.peers[len(self.peers)-p-1].Pos, pos) < 0
	}); len(result) < n; i++ {
		result = append(result, self.peers[len(self.peers)-1-i%len(self.peers)])
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
