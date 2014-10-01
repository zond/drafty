package messages

type PutRequest struct {
	Logs  string
	Key   []byte
	Value []byte
}

type HashesRequest struct {
	Prefix []byte
	Level  uint
}
