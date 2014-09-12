package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/spaolacci/murmur3"
)

var valueBucketKey = []byte("values")
var merkleBucketKey = []byte("merkles")
var maxLevelKey = []byte("maxLevel")

type DB struct {
	bolt *bolt.DB
}

func New(file string) (result *DB, err error) {
	result = &DB{}
	if result.bolt, err = bolt.Open(file, 0600, nil); err != nil {
		return
	}
	return
}

func (self *DB) concat(h1 uint64, h2 uint64) []byte {
	return []byte{
		byte(h1 >> 56), byte(h1 >> 48), byte(h1 >> 40), byte(h1 >> 32),
		byte(h1 >> 24), byte(h1 >> 16), byte(h1 >> 8), byte(h1),
		byte(h2 >> 56), byte(h2 >> 48), byte(h2 >> 40), byte(h2 >> 32),
		byte(h2 >> 24), byte(h2 >> 16), byte(h2 >> 8), byte(h2),
	}
}

func merkleLevelKey(l int) (result []byte) {
	result = make([]byte, binary.MaxVarintLen64)
	result = result[:binary.PutUvarint(result, uint64(l))]
	return
}

func levelStart(level int, key []byte) (result []byte) {
	result = make([]byte, len(key))
	copy(result, key)
	mask := byte(0xf0)
	for i := 0; i < level && i < len(key)*2; i++ {
		result[len(result)-1-i/2] &= mask
		mask = 0xff ^ mask
	}
	return
}

func levelEnd(level int, key []byte) (result []byte) {
	result = levelStart(level, key)
	if len(result) < 1+level/2 {
		newRes := make([]byte, 1+level/2)
		copy(newRes, result)
		result = newRes
	}
	pos := len(result) - 1 - level/2
	result[pos] += 1 << (4 * (uint(level) % 2))
	for result[pos] == 0 {
		result[pos] += 1
		if result[pos] == 0 && pos == 0 {
			newRes := make([]byte, len(result)+1)
			copy(newRes[1:], result)
			result = newRes
		} else {
			pos--
		}
	}
	return
}

func (self *DB) hash(bucket *bolt.Bucket, key []byte, level int) (result []byte, start []byte, err error) {
	values := bucket.Cursor()
	start = levelStart(level, key)
	end := levelEnd(level, key)
	hash := murmur3.New128()
	for k, v := values.Seek(start); k != nil && bytes.Compare(k, end) < 0; k, v = values.Next() {
		if _, err = hash.Write(v); err != nil {
			return
		}
	}
	result = self.concat(hash.Sum128())
	return
}

func (self *DB) getUint(bucket *bolt.Bucket, key []byte) (result int) {
	i, _ := binary.Varint(bucket.Get(key))
	result = int(i)
	return
}

func (self *DB) putUint(bucket *bolt.Bucket, key []byte, i int) (err error) {
	b := make([]byte, binary.MaxVarintLen64)
	b = b[:binary.PutVarint(b, int64(i))]
	return bucket.Put(key, b)
}

func (self *DB) updateMerkle(valueBucket *bolt.Bucket, merkleBucket *bolt.Bucket, key []byte) (err error) {
	level := 1
	maxLevel := self.getUint(merkleBucket, maxLevelKey)
	hash, levelStart, err := self.hash(valueBucket, key, level)
	if err != nil {
		return
	}
	merkles, err := merkleBucket.CreateBucketIfNotExists(merkleLevelKey(level))
	if err != nil {
		return
	}
	if err = merkles.Put(levelStart, hash); err != nil {
		return
	}
	level++
	for valueBucket = merkles; valueBucket.Stats().KeyN > 1; valueBucket = merkles {
		if level > maxLevel {
			if err = self.putUint(merkleBucket, maxLevelKey, level); err != nil {
				return
			}
		}
		if merkles, err = merkleBucket.CreateBucketIfNotExists(merkleLevelKey(level)); err != nil {
			return
		}
		if hash, levelStart, err = self.hash(valueBucket, key, level); err != nil {
			return
		}
		if err = merkles.Put(levelStart, hash); err != nil {
			return
		}
		level++
	}
	return
}

func (self *DB) Put(key []byte, value []byte) (err error) {
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
