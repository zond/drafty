package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/spaolacci/murmur3"
)

var valueBucketKey = []byte("values")
var merkleBucketKey = []byte("merkles")

type DB interface {
	Hash() ([]byte, error)
	Equal(DB) (bool, error)
	Close() error
	Put([]byte, []byte) error
	PutString(string, string) error
	Get([]byte) ([]byte, error)
	GetString(string) (string, error)
	Delete([]byte) error
	DeleteString(string) error
}

type db struct {
	bolt *bolt.DB
}

func New(file string) (result DB, err error) {
	d := &db{}
	if d.bolt, err = bolt.Open(file, 0600, nil); err != nil {
		return
	}
	result = d
	return
}

func (self *db) Close() (err error) {
	err = self.bolt.Close()
	return
}

func (self *db) concat(h1 uint64, h2 uint64) []byte {
	return []byte{
		byte(h1 >> 56), byte(h1 >> 48), byte(h1 >> 40), byte(h1 >> 32),
		byte(h1 >> 24), byte(h1 >> 16), byte(h1 >> 8), byte(h1),
		byte(h2 >> 56), byte(h2 >> 48), byte(h2 >> 40), byte(h2 >> 32),
		byte(h2 >> 24), byte(h2 >> 16), byte(h2 >> 8), byte(h2),
	}
}

func merkleLevelKey(l uint) (result []byte) {
	result = make([]byte, binary.MaxVarintLen64)
	result = result[:binary.PutUvarint(result, uint64(l))]
	return
}

func fmtBytes(b []byte) string {
	buf := &bytes.Buffer{}
	for _, b := range b {
		fmt.Fprintf(buf, "%08b", b)
	}
	return buf.String()
}

func levelStart(level uint, key []byte) (result []byte) {
	if level == 0 {
		return []byte{0}
	}
	if level > uint(len(key)) {
		level = uint(len(key))
	}
	return key[:level]
}

func levelEnd(level uint, key []byte) (result []byte) {
	if level == 0 {
		return []byte{0}
	}
	start := levelStart(level, key)
	result = make([]byte, len(start))
	copy(result, start)
	pos := len(result) - 1
	result[pos] = result[pos] + 1
	for result[pos] == 0 {
		if pos == 0 {
			result = []byte{0}
			break
		}
		pos--
		result[pos]++
	}
	return
}

func (self *db) hash(bucket *bolt.Bucket, key []byte, level uint) (result []byte, start []byte, sources int, err error) {
	values := bucket.Cursor()
	start = levelStart(level, key)
	end := levelEnd(level, key)
	toTheEnd := len(end) == 1 && end[0] == 0
	hash := murmur3.New128()
	for k, v := values.Seek(start); k != nil && (toTheEnd || bytes.Compare(k, end) < 0); k, v = values.Next() {
		if _, err = hash.Write(v); err != nil {
			return
		}
		sources++
	}
	result = self.concat(hash.Sum128())
	return
}

func (self *db) getUint(bucket *bolt.Bucket, key []byte) (result uint) {
	k := bucket.Get(key)
	if k == nil {
		return 0
	}
	i, _ := binary.Uvarint(k)
	result = uint(i)
	return
}

func (self *db) putUint(bucket *bolt.Bucket, key []byte, i uint) (err error) {
	b := make([]byte, binary.MaxVarintLen64)
	b = b[:binary.PutUvarint(b, uint64(i))]
	return bucket.Put(key, b)
}

func (self *db) updateMerkle(valueBucket *bolt.Bucket, merkleBucket *bolt.Bucket, key []byte) (err error) {
	level := uint(len(key))
	merkles, err := merkleBucket.CreateBucketIfNotExists(merkleLevelKey(level))
	if err != nil {
		return
	}
	hash, levelStart, sources, err := self.hash(valueBucket, key, level)
	if err != nil {
		return
	}
	if sources > 0 {
		if err = merkles.Put(levelStart, hash); err != nil {
			return
		}
	} else {
		if err = merkles.Delete(levelStart); err != nil {
			return
		}
	}
	for level > 0 {
		valueBucket = merkles
		level--
		if merkles, err = merkleBucket.CreateBucketIfNotExists(merkleLevelKey(level)); err != nil {
			return
		}
		if hash, levelStart, sources, err = self.hash(valueBucket, key, level); err != nil {
			return
		}
		if sources > 0 {
			if err = merkles.Put(levelStart, hash); err != nil {
				return
			}
		} else {
			if err = merkles.Delete(levelStart); err != nil {
				return
			}
		}
	}
	return
}

func (self *db) Equal(o DB) (result bool, err error) {
	h1, err := self.Hash()
	if err != nil {
		return
	}
	h2, err := o.Hash()
	if err != nil {
		return
	}
	result = bytes.Compare(h1, h2) == 0
	return
}

func (self *db) Hash() (result []byte, err error) {
	if err = self.bolt.Update(func(tx *bolt.Tx) (err error) {
		merkleBucket, err := tx.CreateBucketIfNotExists(merkleBucketKey)
		if err != nil {
			return
		}
		_, result = merkleBucket.Bucket(merkleLevelKey(0)).Cursor().First()
		return
	}); err != nil {
		return
	}
	return
}

func (self *db) PutString(key string, value string) (err error) {
	return self.Put([]byte(key), []byte(value))
}

func (self *db) GetString(key string) (result string, err error) {
	res, err := self.Get([]byte(key))
	if err != nil {
		return
	}
	result = string(res)
	return
}

func (self *db) Get(key []byte) (result []byte, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		valueBucket, err := tx.CreateBucketIfNotExists(valueBucketKey)
		if err != nil {
			return
		}
		result = valueBucket.Get(key)
		return
	}); err != nil {
		return
	}
	return
}

func (self *db) DeleteString(key string) (err error) {
	return self.Delete([]byte(key))
}

func (self *db) Delete(key []byte) (err error) {
	if err = self.bolt.Update(func(tx *bolt.Tx) (err error) {
		valueBucket, err := tx.CreateBucketIfNotExists(valueBucketKey)
		if err != nil {
			return
		}
		if err = valueBucket.Delete(key); err != nil {
			return
		}
		merkleBucket, err := tx.CreateBucketIfNotExists(merkleBucketKey)
		if err != nil {
			return
		}
		if err = self.updateMerkle(valueBucket, merkleBucket, key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	return
}
func (self *db) Put(key []byte, value []byte) (err error) {
	if err = self.bolt.Update(func(tx *bolt.Tx) (err error) {
		valueBucket, err := tx.CreateBucketIfNotExists(valueBucketKey)
		if err != nil {
			return
		}
		if err = valueBucket.Put(key, value); err != nil {
			return
		}
		merkleBucket, err := tx.CreateBucketIfNotExists(merkleBucketKey)
		if err != nil {
			return
		}
		if err = self.updateMerkle(valueBucket, merkleBucket, key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	return
}
