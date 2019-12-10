package redisdb

import (
	"bytes"
	"fmt"
	"os"

	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

const redisServer = "127.0.0.1:6379"
const redisPass = "gaojiachenisbazhahei"

// BatchItem is the structure of redis command items
type BatchItem struct {
	command string
	key     []byte
	value   []byte
}

// Batch structure contains a list and redis connection
type Batch struct {
	l []BatchItem
	c redis.Conn
}

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (d *DB) NewBatch() *Batch {
	b := Batch{c: d.c}
	b.l = make([]BatchItem, 0)
	return &b
}

// Replay replays batch contents.
func (b *Batch) Replay(r BatchReplay) error {
	// for _, index := range b.index {
	// 	switch index.keyType {
	// 	case keyTypeVal:
	// 		r.Put(index.k(b.data), index.v(b.data))
	// 	case keyTypeDel:
	// 		r.Delete(index.k(b.data))
	// 	}
	// }
	for _, item := range b.l {
		if item.command == "SET" {
			r.Put(item.key, item.value)
		}
		if item.command == "DEL" {
			r.Delete(item.key)
		}
	}
	return nil
}

// Put inserts the given value into the batch for later committing.
func (b *Batch) Put(key []byte, value []byte) error {
	b.l = append(b.l, BatchItem{command: "SET", key: key, value: value})
	bpsc := 0
	logrus.WithFields(logrus.Fields{
		"set": bpsc,
	}).Debug("RedisBatchSet")
	return nil
}

// Write do all the command in the batch
func (b *Batch) Write() {
	dc, sc := 0, 0
	for _, it := range b.l {
		if it.command == "DEL" {
			b.c.Do(it.command, it.key)
			b.c.Do("SREM", "gs", it.key)
			dc++
		} else {
			b.c.Do(it.command, it.key, it.value)
			b.c.Do("SADD", "gs", it.key)
			sc++
		}
	}
	b.l = b.l[0:0]
	// b.l = //empty
	logrus.WithFields(logrus.Fields{
		"set": sc,
		"del": dc,
	}).Debug("RedisBatchWrite")
}

// Reset resets the batch for reuse.
func (b *Batch) Reset() {
	b.l = b.l[0:0] //empty
}

// Delete inserts the a key removal into the batch for later committing.
func (b *Batch) Delete(key []byte) error {
	b.l = append(b.l, BatchItem{command: "DEL", key: key})
	return nil
}

// DB is a persestent key-value store, contains redis connection
type DB struct {
	c redis.Conn
}

// New returns a wrapped redisDB object.
func New() *DB {
	c, err := redis.Dial("tcp", redisServer, redis.DialDatabase(0), redis.DialPassword(redisPass))
	if err != nil {
		fmt.Println("connect redis error :", err)
		return nil
	}
	return &DB{c}
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (d *DB) Close() error {
	// never close
	return nil
	// return d.c.Close()
}

// Has retrieves if a key is present in the key-value store.
func (d *DB) Has(key []byte) (bool, error) {
	v, err := d.Get(key)
	return v != nil, err
}

// Set inserts the given value into the key-value store.
func (d *DB) Set(key []byte, value []byte) error {
	if d.c.Err() != nil {
		c, err := redis.Dial("tcp", redisServer, redis.DialDatabase(0), redis.DialPassword(redisPass))
		if err != nil {
			logrus.WithField("err", err).Error("ReDial Error")
		} else {
			logrus.Warning("ReDial Succeed")
		}
		d.c = c
	}
	_, err := d.c.Do("SET", key, value)
	if err != nil {
		fmt.Println("redis set failed:", err)
		return err
	}
	_, err = d.c.Do("SADD", "gs", key)
	if err != nil {
		fmt.Println("redis set failed:", err)
		return err
	}
	return nil
}

// Get retrieves the given key if it's present in the key-value store.
func (d *DB) Get(key []byte) ([]byte, error) {
	value, err := redis.Bytes(d.c.Do("GET", key))
	if err != nil {
		return nil, nil
	}
	return value, nil
}

// Del removes the key from the key-value data store.
func (d *DB) Del(key []byte) error {
	_, err := d.c.Do("DEL", key)
	d.c.Do("SREM", "gs", key)
	if err != nil {
		fmt.Println("redis delete failed:", err)
		return err
	}
	return err
}

// RedisIterator structure contains keys array and index for keys
type RedisIterator struct {
	keysCache   [][]byte
	index       int
	redisClient redis.Conn
}

// NewRedisIterator returns the keys from start (included) to limit (not included)
func NewRedisIterator(start []byte, limit []byte) *RedisIterator {
	logrus.Debug("RedisNewIterator")
	it := new(RedisIterator)
	c, err := redis.Dial("tcp", redisServer, redis.DialDatabase(0), redis.DialPassword(redisPass))
	if err != nil {
		fmt.Println("connect redis error :", err)
		return nil
	}
	it.redisClient = c
	k := 0

	keys, _ := redis.ByteSlices(it.redisClient.Do("SORT", "gs", "ALPHA"))
	// keys, _ := redis.ByteSlices(it.redisClient.Do("SORT", "gs", "ALPHA","limit",it.index?,"1"))
	var emptyByte []byte
	for i := 0; i < len(keys); i++ {
		if bytes.Compare(keys[i], start) != 1 {
			if limit == nil || bytes.Equal(limit, emptyByte) || bytes.Compare(keys[i], limit) == -1 {
				it.keysCache[k] = keys[i]
				k++
			}
		}
	}
	return it
}

// NewRedisIteratorWithPrefix creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix.
func NewRedisIteratorWithPrefix(prefix []byte) *RedisIterator {
	return NewRedisIterator(prefix, bytesPrefix(prefix))
}

// BytesPrefix returns key range that satisfy the given prefix.
// This only applicable for the standard 'bytes comparer'.
func bytesPrefix(prefix []byte) []byte {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return limit
}

// Next returns whether there is a next key in Iterator
func (it *RedisIterator) Next() bool {
	it.index++
	logrus.WithFields(logrus.Fields{
		"index": it.index,
		"key":   it.Key(),
	}).Trace("RedisIteratorNext")
	if it.index > 200 {
		os.Exit(1)
	}
	return it.index >= len(it.keysCache)
}

// Key returns the current key of the key/value pair in Iterator
func (it *RedisIterator) Key() []byte {
	if len(it.keysCache) == 0 {
		return []byte{}
	}
	return it.keysCache[it.index]
}

// Value returns the current value of the key/value pair in Iterator
func (it *RedisIterator) Value() []byte {
	if len(it.keysCache) == 0 {
		return []byte{}
	}
	v, _ := it.redisClient.Do("GET", it.Key())
	return v.([]byte)
}

// Release releases associated resources
func (it *RedisIterator) Release() {
	it.keysCache = make([][]byte, 0)
	it.index = 0
	it.redisClient.Close()
}

// Error returns error (nil)
func (it *RedisIterator) Error() error {
	return nil
}

// func (d *DB) Compact (){
// 	//TODO
// }
