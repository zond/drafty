package ring

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/rpc"
	"sort"

	"github.com/zond/drafty/switchboard"
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

func (self *Peer) Call(method string, input interface{}, output interface{}) error {
	return switchboard.Switch.Call(self.ConnectionString, method, input, output)
}

func (self *Peer) Go(method string, args, reply interface{}, done chan *rpc.Call) (call *rpc.Call) {
	return switchboard.Switch.Go(self.ConnectionString, method, args, reply, done)
}

func (self *Peer) Equal(o *Peer) bool {
	return self.Name == o.Name && bytes.Compare(self.Pos, o.Pos) == 0 && self.ConnectionString == o.ConnectionString
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

func (self Peers) ContainsPos(pos []byte) bool {
	for _, peer := range self {
		if bytes.Compare(peer.Pos, pos) == 0 {
			return true
		}
	}
	return false
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

func (self *Ring) String() string {
	return fmt.Sprint(self.peers)
}

func (self *Ring) Equal(o *Ring) bool {
	if len(self.peers) != len(o.peers) {
		return false
	}
	for index, peer := range self.peers {
		if !peer.Equal(o.peers[index]) {
			return false
		}
	}
	return true
}

func (self *Ring) Len() int {
	return len(self.peers)
}

func (self *Ring) Each(f func(*Peer)) {
	for _, peer := range self.peers {
		f(peer)
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

type pubRing struct {
	Peers      Peers
	PeerByName map[string]*Peer
}

func (self *Ring) MarshalText() (b []byte, err error) {
	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(pubRing{
		Peers:      self.peers,
		PeerByName: self.peerByName,
	}); err != nil {
		return
	}
	b = buf.Bytes()
	return
}

func (self *Ring) UnmarshalText(b []byte) (err error) {
	g := &pubRing{}
	if err = json.NewDecoder(bytes.NewBuffer(b)).Decode(g); err != nil {
		return
	}
	self.peers, self.peerByName = g.Peers, g.PeerByName
	return
}

func (self *Ring) MarshalBinary() (b []byte, err error) {
	buf := &bytes.Buffer{}
	if err = gob.NewEncoder(buf).Encode(pubRing{
		Peers:      self.peers,
		PeerByName: self.peerByName,
	}); err != nil {
		return
	}
	b = buf.Bytes()
	return
}

func (self *Ring) UnmarshalBinary(b []byte) (err error) {
	g := &pubRing{}
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
