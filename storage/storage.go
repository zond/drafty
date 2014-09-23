package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/kr/pretty"
	"github.com/spaolacci/murmur3"
)

var valueBucketKey = []byte("values")
var merkleBucketKey = []byte("merkles")

// OverwriteFunc returns whether localValue should overwrite remoteValue
type OverwriteFunc func(localValue, remoteValue []byte) bool

type Synchronizable interface {
	Hash() ([]byte, error)
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
	Hashes([]byte, uint) ([256][]byte, error)
}

type DB interface {
	Synchronizable
	Equal(Synchronizable) (bool, error)
	Close() error
	PutString(string, string) error
	GetString(string) (string, error)
	Delete([]byte) error
	DeleteString(string) error
	Sync(Synchronizable, Range, OverwriteFunc) error
	View(func(*bolt.Bucket) error) error
	PP() string
	ToSortedMap() ([][2][]byte, error)
}

type db struct {
	bolt *bolt.DB
}

func New(file string) (result DB, err error) {
	d := &db{}
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

func (self *db) View(f func(*bolt.Bucket) error) (err error) {
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

func (self *db) hash(valueBucket, hashBucket *bolt.Bucket, key []byte, level uint) (result []byte, start []byte, sources int, err error) {
	hash := murmur3.New128()
	if val := valueBucket.Get(key); val != nil {
		if _, err = hash.Write(val); err != nil {
			return
		}
		sources++
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

func (self *db) updateHashes(valueBucket, merkleBucket *bolt.Bucket, key []byte) (err error) {
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

type Range struct {
	FromInc []byte
	ToExc   []byte
}

func (self Range) Within(k []byte) bool {
	return (self.FromInc == nil || bytes.Compare(self.FromInc, k) < 1) && (self.ToExc == nil || bytes.Compare(self.ToExc, k) > 0)
}

func (self Range) PrefixWithin(k []byte) bool {
	from := self.FromInc
	if len(from) > len(k) {
		from = self.FromInc[:len(k)]
	}
	to := self.ToExc
	if len(to) > len(k) {
		to = self.ToExc[:len(k)]
	}
	return (from == nil || bytes.Compare(from, k) < 1) && (to == nil || bytes.Compare(to, k) > -1)
}

func (self Range) Empty() bool {
	return self.FromInc == nil && self.ToExc == nil
}

func (self *db) sync(o Synchronizable, level uint, prefix []byte, r Range, overwriteFunc OverwriteFunc) (err error) {
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
					var value []byte
					if value, err = self.Get(newPrefix); err != nil {
						return
					}
					var oValue []byte
					if oValue, err = o.Get(newPrefix); err != nil {
						return
					}
					if bytes.Compare(value, oValue) != 0 {
						if overwriteFunc(value, oValue) {
							if err = o.Put(newPrefix, value); err != nil {
								return
							}
						} else {
							if err = self.Put(newPrefix, oValue); err != nil {
								return
							}
						}
					}
				}
				self.sync(o, level+1, newPrefix, r, overwriteFunc)
			}
		}
	}
	return
}

func (self *db) Sync(o Synchronizable, r Range, overwriteFunc OverwriteFunc) (err error) {
	eq, err := self.Equal(o)
	if err != nil {
		return
	}
	if eq {
		return
	}
	return self.sync(o, 1, nil, r, overwriteFunc)
}

func (self *db) Equal(o Synchronizable) (result bool, err error) {
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

func (self *db) PP() string {
	m, err := self.ToSortedMap()
	if err != nil {
		return err.Error()
	}
	return pretty.Sprintf("%# v", m)
}

func (self *db) ToSortedMap() (result [][2][]byte, err error) {
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

func (self *db) Hashes(key []byte, level uint) (result [256][]byte, err error) {
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

func (self *db) Hash() (result []byte, err error) {
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
		result = merkles.Get([]byte{0})
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
		valueBucket := tx.Bucket(valueBucketKey)
		if valueBucket == nil {
			err = fmt.Errorf("Database has no value bucket?")
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
		if err = self.updateHashes(valueBucket, merkleBucket, key); err != nil {
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
		if err = self.updateHashes(valueBucket, merkleBucket, key); err != nil {
			return
		}
		return
	}); err != nil {
		return
	}
	return
}
