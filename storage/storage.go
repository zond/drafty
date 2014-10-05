package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/kr/pretty"
	"github.com/spaolacci/murmur3"
	"github.com/zond/drafty/log"
	"github.com/zond/drafty/ranje"
)

var valueBucketKey = []byte("values")
var merkleBucketKey = []byte("merkles")

type Synchronizable interface {
	Hash() ([]byte, error)
	Put([]byte, Value, string) error
	Get([]byte) (Value, error)
	Hashes([]byte, uint) ([256][]byte, error)
}

const (
	MaxTombstoneAge = time.Hour
)

const (
	deleted = 1 << iota
)

type Values []Value

type Value []byte

func (self Value) WriteTimestamp() (result int64) {
	result += int64(self[8] << 7)
	result += int64(self[9] << 6)
	result += int64(self[10] << 5)
	result += int64(self[11] << 4)
	result += int64(self[12] << 3)
	result += int64(self[13] << 2)
	result += int64(self[14] << 1)
	result += int64(self[15])
	return
}

func (self Value) ReadTimestamp() (result int64) {
	result += int64(self[0] << 7)
	result += int64(self[1] << 6)
	result += int64(self[2] << 5)
	result += int64(self[3] << 4)
	result += int64(self[4] << 3)
	result += int64(self[5] << 2)
	result += int64(self[6] << 1)
	result += int64(self[7])
	return
}

func (self Value) Deleted() bool {
	return self[16]&deleted == deleted
}

func (self Value) Bytes() []byte {
	return self[17:]
}

type DB struct {
	bolt *bolt.DB
}

func New(file string) (result *DB, err error) {
	d := &DB{}
	if d.bolt, err = bolt.Open(file, 0600, nil); err != nil {
		return
	}
	if err = d.bolt.Update(func(t *bolt.Tx) (err error) {
		if _, err = t.CreateBucketIfNotExists(valueBucketKey); err != nil {
			return
		}
		if _, err = t.CreateBucketIfNotExists(merkleBucketKey); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	result = d
	return
}

func (self *DB) View(f func(*bolt.Bucket) error) (err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		bucket := tx.Bucket(valueBucketKey)
		if bucket == nil {
			err = fmt.Errorf("No values in database")
			return
		}
		return f(bucket)
	}); err != nil {
		return
	}
	return
}

func (self *DB) Close() (err error) {
	err = self.bolt.Close()
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
	result = make([]byte, level)
	copy(result, key)
	return
}

func levelEnd(level uint, key []byte) (result []byte) {
	if level == 0 {
		return []byte{0}
	}
	result = levelStart(level, key)
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

func (self *DB) hash(valueBucket, hashBucket *bolt.Bucket, key []byte, level uint) (result []byte, start []byte, sources int, err error) {
	cutoff := time.Now().Add(-MaxTombstoneAge).UnixNano()
	hash := murmur3.New128()
	if val := Value(valueBucket.Get(key)); val != nil {
		if val.Deleted() {
			if val.ReadTimestamp() < cutoff {
				if err = valueBucket.Delete(key); err != nil {
					return
				}
			}
		} else {
			if _, err = hash.Write(key); err != nil {
				return
			}
			if _, err = hash.Write(val); err != nil {
				return
			}
			sources++
		}
	}
	start = levelStart(level, key)
	end := levelEnd(level, key)
	toTheEnd := len(end) == 1 && end[0] == 0
	sourceHashes := hashBucket.Cursor()
	for k, v := sourceHashes.Seek(start); k != nil && (toTheEnd || bytes.Compare(k, end) < 0); k, v = sourceHashes.Next() {
		if _, err = hash.Write(v); err != nil {
			return
		}
		sources++
	}
	result = self.concat(hash.Sum128())
	return
}

func (self *DB) getUint(bucket *bolt.Bucket, key []byte) (result uint) {
	k := bucket.Get(key)
	if k == nil {
		return 0
	}
	i, _ := binary.Uvarint(k)
	result = uint(i)
	return
}

func (self *DB) putUint(bucket *bolt.Bucket, key []byte, i uint) (err error) {
	b := make([]byte, binary.MaxVarintLen64)
	b = b[:binary.PutUvarint(b, uint64(i))]
	return bucket.Put(key, b)
}

func (self *DB) updateHashes(valueBucket, merkleBucket *bolt.Bucket, key []byte) (err error) {
	var srcHashBucket *bolt.Bucket
	var dstHashBucket *bolt.Bucket
	var hash []byte
	var levelStart []byte
	var sources int
	for level := len(key); level >= 0; level-- {
		if srcHashBucket, err = merkleBucket.CreateBucketIfNotExists(merkleLevelKey(uint(level + 1))); err != nil {
			return
		}
		if hash, levelStart, sources, err = self.hash(valueBucket, srcHashBucket, key[:level], uint(level)); err != nil {
			return
		}
		if dstHashBucket, err = merkleBucket.CreateBucketIfNotExists(merkleLevelKey(uint(level))); err != nil {
			return
		}
		if sources > 0 {
			if err = dstHashBucket.Put(levelStart, hash); err != nil {
				return
			}
		} else {
			if err = dstHashBucket.Delete(levelStart); err != nil {
				return
			}
		}
	}
	return
}

func (self *DB) sync(o Synchronizable, level uint, prefix []byte, r ranje.Range, maxOps uint64, logs string) (ops uint64, err error) {
	hashes, err := self.Hashes(prefix, level)
	if err != nil {
		return
	}
	oHashes, err := o.Hashes(prefix, level)
	if err != nil {
		return
	}
	newPrefix := make([]byte, len(prefix)+1)
	copy(newPrefix, prefix)
	for i := 0; i < 256; i++ {
		newPrefix[len(prefix)] = byte(i)
		if r.PrefixWithin(newPrefix) {
			if bytes.Compare(hashes[i], oHashes[i]) != 0 {
				if r.Within(newPrefix) {
					log.Tracef("%v found %v to be different", logs, hex.EncodeToString(newPrefix))
					var value Value
					if value, err = self.Get(newPrefix); err != nil {
						return
					}
					var oValue Value
					if oValue, err = o.Get(newPrefix); err != nil {
						return
					}
					if bytes.Compare(value, oValue) != 0 {
						vRTS := int64(-1)
						oRTS := int64(-1)
						if value != nil {
							vRTS = value.ReadTimestamp()
						}
						if oValue != nil {
							oRTS = oValue.ReadTimestamp()
						}
						dst, target := o, value
						if vRTS < oRTS {
							dst, target = self, oValue
						}
						log.Tracef("%v sync putting %v => %v\n", logs, hex.EncodeToString(newPrefix), target)
						if err = dst.Put(newPrefix, target, logs); err != nil {
							return
						}
						ops++
						if ops >= maxOps {
							return
						}
					}
				}
				var newOps uint64
				if newOps, err = self.sync(o, level+1, newPrefix, r, maxOps-ops, logs); err != nil {
					return
				}
				ops += newOps
				if ops >= maxOps {
					return
				}
			}
		}
	}
	return
}

func (self *DB) SyncAll(o Synchronizable, r ranje.Range) (err error) {
	var ops uint64
	for ops, err = self.Sync(o, r, uint64(0xffffffffffffffff), ""); err == nil && ops > 0; ops, err = self.Sync(o, r, uint64(0xffffffffffffffff), "") {
	}
	return
}

func (self *DB) Sync(o Synchronizable, r ranje.Range, maxOps uint64, logs string) (ops uint64, err error) {
	eq, err := self.Equal(o)
	if err != nil {
		return
	}
	if eq {
		return
	}
	return self.sync(o, 1, nil, r, maxOps, logs)
}

func (self *DB) Equal(o Synchronizable) (result bool, err error) {
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

func (self *DB) PPStrings() string {
	m, err := self.ToSortedMapStrings()
	if err != nil {
		return err.Error()
	}
	return pretty.Sprintf("%# v", m)
}

func (self *DB) PP() string {
	m, err := self.ToSortedMap()
	if err != nil {
		return err.Error()
	}
	return pretty.Sprintf("%# v", m)
}

func (self *DB) ToSortedMapStrings() (result [][2]string, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		valueBucket := tx.Bucket(valueBucketKey)
		if valueBucket == nil {
			err = fmt.Errorf("Database has no value bucket?")
			return
		}
		cursor := valueBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			result = append(result, [2]string{
				string(k),
				string(v[17:]),
			})
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *DB) ToSortedMap() (result [][2][]byte, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		valueBucket := tx.Bucket(valueBucketKey)
		if valueBucket == nil {
			err = fmt.Errorf("Database has no value bucket?")
			return
		}
		cursor := valueBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			result = append(result, [2][]byte{
				k,
				v,
			})
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *DB) Hashes(key []byte, level uint) (result [256][]byte, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		merkleBucket := tx.Bucket(merkleBucketKey)
		if merkleBucket == nil {
			err = fmt.Errorf("Database has no merkle bucket?")
			return
		}
		merkles := merkleBucket.Bucket(merkleLevelKey(level))
		if merkles == nil {
			return
		}
		cursor := merkles.Cursor()
		start := levelStart(level-1, key)
		end := levelEnd(level-1, key)
		toTheEnd := len(end) == 1 && end[0] == 0
		for k, v := cursor.Seek(start); k != nil && (toTheEnd || bytes.Compare(k, end) < 0); k, v = cursor.Next() {
			newV := make([]byte, len(v))
			copy(newV, v)
			result[k[int(level-1)]] = newV
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *DB) Hash() (result []byte, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		merkleBucket := tx.Bucket(merkleBucketKey)
		if merkleBucket == nil {
			err = fmt.Errorf("Database has no merkle bucket?")
			return
		}
		merkles := merkleBucket.Bucket(merkleLevelKey(0))
		if merkles == nil {
			return
		}
		result = append([]byte{}, merkles.Get([]byte{0})...)
		return
	}); err != nil {
		return
	}
	return
}

func (self *DB) PutString(key string, value string) (err error) {
	val := make([]byte, 17+len(value))
	copy(val[17:], value)
	return self.Put([]byte(key), val, "")
}

func (self *DB) GetString(key string) (result string, found bool, err error) {
	res, err := self.Get([]byte(key))
	if err != nil {
		return
	}
	if len(res) == 0 {
		return
	}
	found = true
	result = string(res[17:])
	return
}

func (self *DB) Get(key []byte) (result Value, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		valueBucket := tx.Bucket(valueBucketKey)
		if valueBucket == nil {
			err = fmt.Errorf("Database has no value bucket?")
			return
		}
		val := valueBucket.Get(key)
		if val != nil {
			result = Value(append([]byte{}, val...))
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *DB) Range(r ranje.Range) (result Values, err error) {
	if err = self.bolt.View(func(tx *bolt.Tx) (err error) {
		valueBucket := tx.Bucket(valueBucketKey)
		if valueBucket == nil {
			err = fmt.Errorf("Database has no value bucket?")
			return
		}
		if r.Empty() {
			return
		}
		cursor := valueBucket.Cursor()
		if r.Reversed() {
			for key, value := cursor.Seek(r.FromInc); key != nil; key, value = cursor.Next() {
				result = append(result, Value(append([]byte{}, value...)))
			}
			for key, value := cursor.First(); key != nil && bytes.Compare(key, r.ToExc) < 0; key, value = cursor.Next() {
				result = append(result, Value(append([]byte{}, value...)))
			}
		} else {
			for key, value := cursor.Seek(r.FromInc); key != nil && bytes.Compare(key, r.ToExc) < 0; key, value = cursor.Next() {
				result = append(result, Value(append([]byte{}, value...)))
			}
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *DB) DeleteString(key string) (err error) {
	return self.Delete([]byte(key))
}

func (self *DB) Delete(key []byte) (err error) {
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
		if err = self.updateHashes(valueBucket, merkleBucket, key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	return
}
func (self *DB) Put(key []byte, value Value, logs string) (err error) {
	if len(value) < 17 {
		err = fmt.Errorf("%v needs to be at least 17 bytes long to have a read timestamp (first 8 bytes), write timestamp (next 8 bytes) and flag byte", value)
		return
	}
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
		if err = self.updateHashes(valueBucket, merkleBucket, key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	log.Tracef("%v inserted %v => %v\n", logs, hex.EncodeToString(key), value)
	return
}
