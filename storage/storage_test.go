package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func withDB(t testing.TB, f func(DB)) {
	fname := fmt.Sprintf("test-%v.db", rand.Int())
	db, err := New(fname)
	if err != nil {
		t.Fatalf("%v", err)
	}
	f(db)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("%v", err)
		}
		if err := os.Remove(fname); err != nil {
			t.Fatalf("%v", err)
		}
	}()

}

func TestLevels(t *testing.T) {
	for i := 0; i < 100; i++ {
		key := make([]byte, 1+rand.Int()%32)
		for index, _ := range key {
			key[index] = byte(rand.Int())
		}
		for lvl := 1; lvl < 24; lvl++ {
			start := levelStart(uint(lvl), key)
			end := levelEnd(uint(lvl), key)
			if bytes.Compare(start, key) > 0 {
				t.Errorf("Start level of %v for %v is %v, but that is greater than the key", lvl, fmtBytes(key), fmtBytes(start))
			}
			if (len(end) != 1 || end[0] != 0) && bytes.Compare(end, key) < 1 {
				t.Errorf("End of level %v for %v is %v, but that is not greater than the key", lvl, fmtBytes(key), fmtBytes(end))
			}
		}
	}
}

func TestPartialSync(t *testing.T) {
	for i := 0; i < 10; i++ {
		withDB(t, func(db1 DB) {
			withDB(t, func(db2 DB) {
				for i := 0; i < 1000; i++ {
					if err := db1.Put(randomBytes(4), randomBytes(4)); err != nil {
						return
					}
					if err := db1.Delete(randomBytes(4)); err != nil {
						return
					}
				}
				from := randomBytes(4)
				var to []byte
				for to == nil || bytes.Compare(to, from) < 0 {
					to = randomBytes(4)
				}
				r := Range{
					FromInc: from,
					ToExc:   to,
				}
				if err := db1.Sync(db2, r, copyForwardFunc); err != nil {
					t.Fatalf("%v", err)
				}
				if err := db1.View(func(b1 *bolt.Bucket) (err error) {
					return db2.View(func(b2 *bolt.Bucket) (err error) {
						c1 := b1.Cursor()
						for k1, v1 := c1.First(); k1 != nil; k1, v1 = c1.Next() {
							if r.Within(k1) {
								if bytes.Compare(v1, b2.Get(k1)) != 0 {
									t.Errorf("Synced %+v. Wanted %v => %v to be synced, but got %v", r, k1, v1, b2.Get(k1))
								}
							} else {
								if v2 := b2.Get(k1); v2 != nil {
									t.Errorf("Synced %+v. Wanted %v => %v to NOT be synced, but got %v", r, k1, v1, b2.Get(k1))
								}
							}
						}
						c2 := b2.Cursor()
						for k2, v2 := c2.First(); k2 != nil; k2, v2 = c2.Next() {
							if bytes.Compare(v2, b1.Get(k2)) != 0 {
								t.Errorf("Wrong value")
							}
							if !r.Within(k2) {
								t.Errorf("Wrong key")
							}
						}
						fmt.Printf("Successfully synced %v keys within %+v\n", b2.Stats().KeyN, r)
						return
					})
				}); err != nil {
					t.Fatalf("%v", err)
				}
			})
		})
	}
}

func TestRange(t *testing.T) {
	r := Range{
		FromInc: []byte{1, 2, 3},
		ToExc:   []byte{2, 3, 4},
	}
	if r.Within([]byte{1, 2, 2}) {
		t.Errorf("noo")
	}
	if r.Within([]byte{1, 2, 2, 9, 9, 9}) {
		t.Errorf("noo")
	}
	if r.Within([]byte{2, 3, 4}) {
		t.Errorf("noo")
	}
	if r.Within([]byte{2, 3, 4, 0}) {
		t.Errorf("noo")
	}
	if !r.Within([]byte{1, 2, 3}) {
		t.Errorf("noo")
	}
	if !r.Within([]byte{1, 2, 3, 4}) {
		t.Errorf("noo")
	}
	if !r.Within([]byte{2, 3, 3, 9, 9}) {
		t.Errorf("noo")
	}
}

func randomBytes(l int) (result []byte) {
	result = make([]byte, 1+rand.Int()%(l-1))
	for index, _ := range result {
		result[index] = byte(rand.Int())
	}
	return
}

func copyForwardFunc(a, b []byte) bool {
	if a == nil {
		return false
	}
	return true
}

func TestSync(t *testing.T) {
	withDB(t, func(db1 DB) {
		withDB(t, func(db2 DB) {
			for i := 0; i < 1000; i++ {
				if err := db1.Put(randomBytes(4), randomBytes(4)); err != nil {
					return
				}
				if err := db1.Delete(randomBytes(4)); err != nil {
					return
				}
				if err := db2.Put(randomBytes(4), randomBytes(4)); err != nil {
					return
				}
				if err := db2.Delete(randomBytes(4)); err != nil {
					return
				}
			}
			if err := db1.Sync(db2, Range{}, copyForwardFunc); err != nil {
				t.Fatalf("%v", err)
			}
			if eq, err := db2.Equal(db2); err != nil {
				t.Fatalf("%v", err)
			} else if !eq {
				t.Errorf("Not equal")
			}
			m1, err := db1.ToSortedMap()
			if err != nil {
				t.Fatalf("%v", err)
			}
			m2, err := db2.ToSortedMap()
			if err != nil {
				t.Fatalf("%v", err)
			}
			if !reflect.DeepEqual(m1, m2) {
				t.Errorf("\n%v\n!=\n%v", m1, m2)
			}
		})
	})
}

func BenchmarkPut(b *testing.B) {
	withDB(b, func(db DB) {
		b.StopTimer()
		var keys [][]byte
		var values [][]byte
		for i := 0; i < b.N; i++ {
			keys = append(keys, randomBytes(10))
			values = append(values, randomBytes(10))
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			db.Put(keys[i], values[i])
		}
		b.StopTimer()
	})
}

func TestTopHash(t *testing.T) {
	keys := [][]byte{}
	values := [][]byte{}
	toDelete := [][]byte{}
	for i := 0; i < 100; i++ {
		key := randomBytes(10)
		keys = append(keys, key)
		if rand.Int()%2 == 0 {
			toDelete = append(toDelete, key)
		}
		values = append(values, randomBytes(10))
	}
	withDB(t, func(db1 DB) {
		withDB(t, func(db2 DB) {
			for _, i := range rand.Perm(len(keys)) {
				if err := db1.Put(keys[i], values[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(toDelete)) {
				if err := db1.Delete(toDelete[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(keys)) {
				if err := db2.Put(keys[i], values[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(toDelete)) {
				if err := db2.Delete(toDelete[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			if eq, err := db2.Equal(db2); err != nil {
				t.Fatalf("%v", err)
			} else if !eq {
				t.Errorf("Not equal")
			}
		})
	})
}
