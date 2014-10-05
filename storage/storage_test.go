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
	"github.com/zond/drafty/ranje"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func withDB(t testing.TB, f func(*DB)) {
	fname := fmt.Sprintf("test-%v.db", rand.Int63())
	db, err := New(fname)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("%v", err)
		}
		if err := os.Remove(fname); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	f(db)
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

func TestPartialSyncAll(t *testing.T) {
	for i := 0; i < 10; i++ {
		withDB(t, func(db1 *DB) {
			withDB(t, func(db2 *DB) {
				for i := 0; i < 1000; i++ {
					if err := db1.Put(randomKey(4), randomValue(4), ""); err != nil {
						return
					}
					if err := db1.Delete(randomKey(4)); err != nil {
						return
					}
				}
				from := randomKey(4)
				to := randomKey(4)
				r := ranje.Range{
					FromInc: from,
					ToExc:   to,
				}
				if err := db1.SyncAll(db2, r); err != nil {
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

func randomKey(l int) (result []byte) {
	result = make([]byte, 1+rand.Int()%(l-1))
	for index, _ := range result {
		result[index] = byte(rand.Int())
	}
	return
}

func randomValue(l int) (result Value) {
	result = make([]byte, 17+1+rand.Int()%(l-1))
	for index, _ := range result[17:] {
		result[17+index] = byte(rand.Int())
	}
	return
}

func TestSync(t *testing.T) {
	withDB(t, func(db1 *DB) {
		withDB(t, func(db2 *DB) {
			if err := db1.PutString("a", "a"); err != nil {
				t.Fatalf("%v", err)
			}
			if err := db1.PutString("b", "b"); err != nil {
				t.Fatalf("%v", err)
			}
			if err := db1.PutString("c", "c"); err != nil {
				t.Fatalf("%v", err)
			}
			if ops, err := db1.Sync(db2, ranje.Range{}, 1, ""); err != nil || ops != 1 {
				t.Fatalf("%v", err)
			}
			m2, err := db2.ToSortedMap()
			if err != nil {
				t.Fatalf("%v", err)
			}
			expected := [][2][]byte{
				[2][]byte{
					[]byte{97}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97},
				},
			}
			if !reflect.DeepEqual(m2, expected) {
				t.Errorf("not right!, %v != %v", m2, expected)
			}
		})
	})
}

func TestSyncAll(t *testing.T) {
	withDB(t, func(db1 *DB) {
		withDB(t, func(db2 *DB) {
			for i := 0; i < 1000; i++ {
				if err := db1.Put(randomKey(4), randomValue(4), ""); err != nil {
					t.Fatalf("%v", err)
				}
				if err := db1.Delete(randomKey(4)); err != nil {
					t.Fatalf("%v", err)
				}
				if err := db2.Put(randomKey(4), randomValue(4), ""); err != nil {
					t.Fatalf("%v", err)
				}
				if err := db2.Delete(randomKey(4)); err != nil {
					t.Fatalf("%v", err)
				}
			}
			if err := db1.SyncAll(db2, ranje.Range{}); err != nil {
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
	withDB(b, func(db *DB) {
		b.StopTimer()
		var keys [][]byte
		var values [][]byte
		for i := 0; i < b.N; i++ {
			keys = append(keys, randomKey(10))
			values = append(values, randomValue(10))
		}
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			db.Put(keys[i], values[i], "")
		}
		b.StopTimer()
	})
}

func TestTopHash(t *testing.T) {
	keys := [][]byte{}
	values := [][]byte{}
	toDelete := [][]byte{}
	for i := 0; i < 100; i++ {
		key := randomKey(10)
		keys = append(keys, key)
		if rand.Int()%2 == 0 {
			toDelete = append(toDelete, key)
		}
		values = append(values, randomValue(10))
	}
	withDB(t, func(db1 *DB) {
		withDB(t, func(db2 *DB) {
			for _, i := range rand.Perm(len(keys)) {
				if err := db1.Put(keys[i], values[i], ""); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(toDelete)) {
				if err := db1.Delete(toDelete[i]); err != nil {
					t.Fatalf("%v", err)
				}
			}
			for _, i := range rand.Perm(len(keys)) {
				if err := db2.Put(keys[i], values[i], ""); err != nil {
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

func TestRange(t *testing.T) {
	withDB(t, func(db *DB) {
		if err := db.PutString("a", "a"); err != nil {
			t.Fatalf("%v", err)
		}
		if err := db.PutString("b", "b"); err != nil {
			t.Fatalf("%v", err)
		}
		if err := db.PutString("c", "c"); err != nil {
			t.Fatalf("%v", err)
		}
		if err := db.PutString("d", "d"); err != nil {
			t.Fatalf("%v", err)
		}
		v1, err := db.Range(ranje.Range{
			FromInc: []byte("a"),
			ToExc:   []byte("c"),
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !reflect.DeepEqual(v1, Values{
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97},
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 98},
		}) {
			t.Errorf("Wrong result")
		}
		v2, err := db.Range(ranje.Range{
			FromInc: []byte("b"),
			ToExc:   []byte("d"),
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !reflect.DeepEqual(v2, Values{
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 98},
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99},
		}) {
			t.Errorf("Wrong result")
		}
		v3, err := db.Range(ranje.Range{
			FromInc: []byte("c"),
			ToExc:   []byte("b"),
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !reflect.DeepEqual(v3, Values{
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99},
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100},
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 97},
		}) {
			t.Errorf("Wrong result")
		}
		v4, err := db.Range(ranje.Range{
			FromInc: []byte("d"),
			ToExc:   []byte("a"),
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		if !reflect.DeepEqual(v4, Values{
			Value{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100},
		}) {
			t.Errorf("Wrong result")
		}
	})

}
