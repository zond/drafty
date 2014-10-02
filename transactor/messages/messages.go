package messages

type TXState int

const (
	TXRunning TXState = iota
)

type GetRequest struct {
	TX  []byte
	Key []byte
}

type PrewriteAndValidateRequest struct {
	TX            []byte
	ValueContexts map[string]*ValueContext
}

type TX struct {
	Id []byte
}

type ValueContext struct {
	RW             bool
	UW             map[string]struct{}
	UR             map[string]struct{}
	WriteTimestamp int64
	ReadTimestamp  int64
}

type Value struct {
	Context *ValueContext
	Data    []byte
}

func NewValue() (result *Value) {
	return &Value{
		Context: &ValueContext{
			UW: map[string]struct{}{},
			UR: map[string]struct{}{},
		},
	}
}

type NodeContext struct {
	ValueContexts map[string]*ValueContext
	Values        map[string]*Value
}

func NewNodeContext() (result *NodeContext) {
	result = &NodeContext{
		ValueContexts: map[string]*ValueContext{},
		Values:        map[string]*Value{},
	}
	return
}
