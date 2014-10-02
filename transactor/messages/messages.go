package messages

type TXState int

const (
	TXRunning TXState = iota
)

type GetRequest struct {
	TX  []byte
	Key []byte
}

type TX struct {
	Id []byte
}

type ValueMeta struct {
	UW             [][]byte
	WriteTimestamp int64
	ReadTimestamp  int64
}

type Value struct {
	Data []byte
	Meta *ValueMeta
}

type NodeMeta struct {
	Meta   map[string]*ValueMeta
	Values map[string]*Value
}
