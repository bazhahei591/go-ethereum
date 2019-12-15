package redisdb

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/ethdb/shared"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

const redisServer = "127.0.0.1:16379"
const redisPass = "gaojiachenisbazhahei"
const telemetryServer = "127.0.0.1:6379"
const telemetryPass = "gaojiachenisbazhahei"
const telemetryDb = 1

// BatchItem is the structure of redis command items
type BatchItem struct {
	command string
	key     []byte
	value   []byte
}

// Batch structure contains a list and redis connection
type Batch struct {
	l []BatchItem
	d *DB
	f string
}

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (d *DB) NewBatch() *Batch {
	b := Batch{d: d, f: d.f}
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
			b.d.Del(it.key)
			dc++
		}
		if it.command == "SET" {
			b.d.Set(it.key, it.value)
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
	p  *redis.Pool // for data storage
	tp *redis.Pool // for telemetry
	f  string
}

// New returns a wrapped redisDB object.
func New(file string) *DB {
	// c, err := redis.Dial("tcp", redisServer, redis.DialDatabase(0), redis.DialPassword(redisPass))
	// if err != nil {
	// 	fmt.Println("connect redis error :", err)
	// 	return nil
	// }
	// return &DB{c}
	maxIdle := 30
	// if v, ok := conf["MaxIdle"]; ok {
	// 	maxIdle = int(v.(int64))
	// }
	maxActive := 1000
	timeout := 60 * time.Second
	// if v, ok := conf["MaxActive"]; ok {
	// 	maxActive = int(v.(int64))
	// }
	// MaxIdleTimeout := 60 * time.Second
	// 建立连接池
	// conf := config.Get("redis." + name).(*toml.TomlTree)
	pool := &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: 240 * time.Second,
		Wait:        true, //如果超过最大连接，是报错，还是等待。
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServer,
				redis.DialPassword(redisPass),
				redis.DialDatabase(0),
				redis.DialConnectTimeout(timeout),
				redis.DialReadTimeout(timeout),
				redis.DialWriteTimeout(timeout))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	telemetryPool := &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: 240 * time.Second,
		Wait:        true, //如果超过最大连接，是报错，还是等待。
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", telemetryServer,
				redis.DialPassword(telemetryPass),
				redis.DialDatabase(telemetryDb))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	return &DB{pool, telemetryPool, file}
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (d *DB) Close() error {
	// never close
	return nil
}

// Has retrieves if a key is present in the key-value store.
func (d *DB) Has(key []byte) (bool, error) {
	c := d.p.Get()
	defer c.Close()
	tc := d.tp.Get()
	defer tc.Close()
	keyType, _ := shared.KeyType(key)
	tc.Do("INCR", "has-"+d.f)
	tc.Do("INCR", "has-"+keyType)
	tc.Do("INCR", "has-"+keyType+d.f)
	return redis.Bool(c.Do("SISMEMBER", d.f, key))
}

// Set inserts the given value into the key-value store.
func (d *DB) Set(key []byte, value []byte) error {
	c := d.p.Get()
	defer c.Close()
	tc := d.tp.Get()
	defer tc.Close()
	keyType, _ := shared.KeyType(key)
	tc.Do("INCR", "set-"+d.f)
	tc.Do("INCR", "set-"+keyType)
	tc.Do("INCR", "set-"+keyType+d.f)
	exist, err := redis.Bool(c.Do("EXISTS", key))
	if exist { // for debug use, should be delete
		getValue, err := redis.Bytes(c.Do("GET", key))
		if err != nil || !bytes.Equal(getValue, value) {
			logrus.WithFields(logrus.Fields{
				"key":           fmt.Sprintf("%s", key),
				"valueGet":      getValue,
				"valueSet":      value,
				"len(key)":      len(key),
				"len(valueGet)": len(getValue),
				"len(valueSet)": len(value),
				"err":           err,
			}).Error("get set not equal")
			//os.Exit(1) // may be cased by replayer
			exist = false // WARNING: TODO: to update data
			tc.Do("INCR", "error-getsetnotequal-"+d.f)
			tc.Do("INCR", "error-getsetnotequal")
		}
	}
	if !exist {
		_, err := c.Do("SET", key, value)
		if err != nil {
			fmt.Println("redis set failed:", err)
			return err
		}
	}
	_, err = c.Do("SADD", d.f, key)
	if err != nil {
		fmt.Println("redis sadd failed:", err)
		return err
	}
	return nil
}

// Get retrieves the given key if it's present in the key-value store.
func (d *DB) Get(key []byte) ([]byte, error) {
	c := d.p.Get()
	defer c.Close()
	tc := d.tp.Get()
	defer tc.Close()
	keyType, _ := shared.KeyType(key)
	tc.Do("INCR", "get-"+d.f)
	tc.Do("INCR", "get-"+keyType)
	tc.Do("INCR", "get-"+keyType+d.f)
	exist, _ := redis.Bool(c.Do("SISMEMBER", d.f, key))
	if !exist {
		return nil, errors.New("get from redis not exist for this node")
	}
	value, err := redis.Bytes(c.Do("GET", key))
	if err != nil {
		return nil, errors.New("get from redis not exist")
	}
	return value, nil
}

// Del removes the key from the key-value data store.
func (d *DB) Del(key []byte) error {
	c := d.p.Get()
	defer c.Close()
	tc := d.tp.Get()
	defer tc.Close()
	keyType, _ := shared.KeyType(key)
	tc.Do("INCR", "del-"+d.f)
	tc.Do("INCR", "del-"+keyType)
	tc.Do("INCR", "del-"+keyType+d.f)
	// Don't DEL on redis
	// _, err := c.Do("DEL", key)
	_, err := c.Do("SREM", d.f, key)
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
	f           string
}

// NewRedisIterator returns the keys from start (included) to limit (not included)
func (d *DB) NewRedisIterator(start []byte, limit []byte) *RedisIterator {
	logrus.Debug("RedisNewIterator")
	it := new(RedisIterator)
	c, err := redis.Dial("tcp", redisServer, redis.DialDatabase(0), redis.DialPassword(redisPass))
	if err != nil {
		fmt.Println("connect redis error :", err)
		return nil
	}
	it.redisClient = c
	k := 0

	keys, _ := redis.ByteSlices(it.redisClient.Do("SORT", d.f, "ALPHA"))
	// keys, _ := redis.ByteSlices(it.redisClient.Do("SORT", d.f, "ALPHA","limit",it.index?,"1"))
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
func (d *DB) NewRedisIteratorWithPrefix(prefix []byte) *RedisIterator {
	return d.NewRedisIterator(prefix, bytesPrefix(prefix))
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
