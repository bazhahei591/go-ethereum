// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// 这是G文件，用于判断key值以便筛选是否共享

// +build !js

// Package leveldb implements the key-value database layer based on LevelDB.
package leveldb

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/redisdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	// degradationWarnInterval specifies how often warning should be printed if the
	// leveldb database cannot keep up with requested writes.
	degradationWarnInterval = time.Minute

	// minCache is the minimum amount of memory in megabytes to allocate to leveldb
	// read and write caching, split half and half.
	minCache = 16

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16

	// metricsGatheringInterval specifies the interval to retrieve leveldb database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	fn string // filename for reporting
	// rdb *
	db  *leveldb.DB
	rdb *redisdb.DB

	compTimeMeter      metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter      metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter     metrics.Meter // Meter for measuring the data written during compaction
	writeDelayNMeter   metrics.Meter // Meter for measuring the write delay number due to database compaction
	writeDelayMeter    metrics.Meter // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge      metrics.Gauge // Gauge for tracking the size of all the levels in the database
	diskReadMeter      metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter     metrics.Meter // Meter for measuring the effective amount of data written
	memCompGauge       metrics.Gauge // Gauge for tracking the number of memory compaction
	level0CompGauge    metrics.Gauge // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge metrics.Gauge // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge      metrics.Gauge // Gauge for tracking the number of table compaction caused by read opt

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

var (
	// databaseVerisionKey tracks the current database version.
	databaseVerisionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix  = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits

	preimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).

	// BloomBitsIndexPrefix is the data table of a chain indexer to track its progress
	BloomBitsIndexPrefix = []byte("iB")

	preimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
	preimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)
)

// choose which key-value to be shared, which means use redis to store
func isStorageShare(key []byte) bool {
	switch {
	case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix):
		return true
	case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix):
		return true
	case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
		return true
	case bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength):
		return true
	case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
		return true
	case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
		return true
	case bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength):
		return true
	case bytes.HasPrefix(key, preimagePrefix) && len(key) == (len(preimagePrefix)+common.HashLength):
		return true
	case bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength):
		return true
	case bytes.HasPrefix(key, []byte("clique-")) && len(key) == 7+common.HashLength:
		return true
	case bytes.HasPrefix(key, []byte("cht-")) && len(key) == 4+common.HashLength:
		return true
	case bytes.HasPrefix(key, []byte("blt-")) && len(key) == 4+common.HashLength:
		return true
	case len(key) == common.HashLength:
		return true
	default:
		return false
	}
}

// New returns a wrapped LevelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, cache int, handles int, namespace string) (*Database, error) {
	// Ensure we have some minimal caching and file guarantees
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles)

	// Open the db and recover any potential corruptions
	db, err := leveldb.OpenFile(file, &opt.Options{
		OpenFilesCacheCapacity: handles,
		BlockCacheCapacity:     cache / 2 * opt.MiB,
		WriteBuffer:            cache / 4 * opt.MiB, // Two of these are used internally
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
	})
	// rdb_id = 2
	rdb := redisdb.New()
	// fmt.Println("^^^^^^^^^^^^^", file, namespace)

	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(file, nil)
	}
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	ldb := &Database{
		fn:       file,
		db:       db,
		rdb:      rdb,
		log:      logger,
		quitChan: make(chan chan error),
	}
	ldb.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	ldb.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	ldb.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	ldb.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	ldb.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	ldb.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)
	ldb.writeDelayMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/duration", nil)
	ldb.writeDelayNMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/counter", nil)
	ldb.memCompGauge = metrics.NewRegisteredGauge(namespace+"compact/memory", nil)
	ldb.level0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/level0", nil)
	ldb.nonlevel0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/nonlevel0", nil)
	ldb.seekCompGauge = metrics.NewRegisteredGauge(namespace+"compact/seek", nil)

	// Start up the metrics gathering and return
	go ldb.meter(metricsGatheringInterval)
	return ldb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
		db.quitChan = nil
	}
	db.rdb.Close()
	return db.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	if isStorageShare(key) {
		return db.rdb.Has(key)
	}
	return db.db.Has(key, nil)
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	if isStorageShare(key) {
		return db.rdb.Get(key)
	}

	dat, err := db.db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return dat, nil
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	if isStorageShare(key) {
		return db.rdb.Set(key, value)
	}
	return db.db.Put(key, value, nil)
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	if isStorageShare(key) {
		return db.rdb.Del(key)
	}
	return db.db.Delete(key, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	lb := lbatch{
		db: db.db,
		b:  new(leveldb.Batch),
	}
	rb := rbatch{
		rdb: db.rdb,
		rb:  new(redisdb.Batch),
	}
	return &mybatch{
		lb: lb, rb: rb,
	}
}

// combIter contains l and r which represents leveldb Iterator and redisdb Iterator,
// and also the key and value of the current Iterator to be returned.
type combIter struct {
	l     ethdb.Iterator
	r     ethdb.Iterator
	key   []byte
	value []byte
}

func newCombIter(l, r ethdb.Iterator) *combIter {
	c := combIter{
		l: l,
		r: r,
	}
	c.Next()
	return &c
}

// Next is the Next function for combIter, current key to be returned
// considered both redis and leveldb
func (c *combIter) Next() bool {
	if bytes.Compare(c.l.Key(), c.r.Key()) == -1 { //<
		c.key = c.l.Key()
		c.value = c.l.Value()
		c.l.Next()
	} else {
		c.key = c.r.Key()
		c.value = c.r.Value()
		c.r.Next()
	}
	//TODO : after r/l end
	return true
}

// Error returns any accumulated error.
func (c *combIter) Error() error {
	return c.l.Error()
}

// Key returns the key of the current key/value pair, or nil if done.
func (c *combIter) Key() []byte {
	return c.key
}

// Value returns the value of the current key/value pair, or nil if done.
func (c *combIter) Value() []byte {
	return c.value
}

// Release releases associated resources.
func (c *combIter) Release() {
	c.l.Release()
	c.r.Release()
}

// NewIterator creates a binary-alphabetical iterator over the entire keyspace
// contained within the leveldb database.
func (db *Database) NewIterator() ethdb.Iterator {

	//redis return
	redisdbIter := redisdb.NewRedisIterator(nil, nil)
	//leveldb return
	leveldbIter := db.db.NewIterator(new(util.Range), nil)
	//combine
	return newCombIter(leveldbIter, redisdbIter)
}

// NewIteratorWithStart creates a binary-alphabetical iterator over a subset of
// database content starting at a particular initial key (or after, if it does
// not exist).
func (db *Database) NewIteratorWithStart(start []byte) ethdb.Iterator {

	redisdbIter := redisdb.NewRedisIterator(start, nil)
	leveldbIter := db.db.NewIterator(&util.Range{Start: start}, nil)

	return newCombIter(leveldbIter, redisdbIter)
}

// NewIteratorWithPrefix creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix.
func (db *Database) NewIteratorWithPrefix(prefix []byte) ethdb.Iterator {

	redisdbIter := redisdb.NewRedisIteratorWithPrefix(prefix)
	leveldbIter := db.db.NewIterator(util.BytesPrefix(prefix), nil)
	return newCombIter(leveldbIter, redisdbIter)
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return db.db.GetProperty(property)
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	return db.db.CompactRange(util.Range{Start: start, Limit: limit})
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.fn
}

// meter periodically retrieves internal leveldb counters and reports them to
// the metrics subsystem.
//
// This is how a LevelDB stats table looks like (currently):
//   Compactions
//    Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)
//   -------+------------+---------------+---------------+---------------+---------------
//      0   |          0 |       0.00000 |       1.27969 |       0.00000 |      12.31098
//      1   |         85 |     109.27913 |      28.09293 |     213.92493 |     214.26294
//      2   |        523 |    1000.37159 |       7.26059 |      66.86342 |      66.77884
//      3   |        570 |    1113.18458 |       0.00000 |       0.00000 |       0.00000
//
// This is how the write delay look like (currently):
// DelayN:5 Delay:406.604657ms Paused: false
//
// This is how the iostats look like (currently):
// Read(MB):3895.04860 Write(MB):3654.64712
func (db *Database) meter(refresh time.Duration) {
	// Create the counters to store current and previous compaction values
	compactions := make([][]float64, 2)
	for i := 0; i < 2; i++ {
		compactions[i] = make([]float64, 4)
	}
	// Create storage for iostats.
	var iostats [2]float64

	// Create storage and warning log tracer for write delay.
	var (
		delaystats      [2]int64
		lastWritePaused time.Time
	)

	var (
		errc chan error
		merr error
	)

	// Iterate ad infinitum and collect the stats
	for i := 1; errc == nil && merr == nil; i++ {
		// Retrieve the database stats
		stats, err := db.db.GetProperty("leveldb.stats")
		if err != nil {
			db.log.Error("Failed to read database stats", "err", err)
			merr = err
			continue
		}
		// Find the compaction table, skip the header
		lines := strings.Split(stats, "\n")
		for len(lines) > 0 && strings.TrimSpace(lines[0]) != "Compactions" {
			lines = lines[1:]
		}
		if len(lines) <= 3 {
			db.log.Error("Compaction leveldbTable not found")
			merr = errors.New("compaction leveldbTable not found")
			continue
		}
		lines = lines[3:]

		// Iterate over all the leveldbTable rows, and accumulate the entries
		for j := 0; j < len(compactions[i%2]); j++ {
			compactions[i%2][j] = 0
		}
		for _, line := range lines {
			parts := strings.Split(line, "|")
			if len(parts) != 6 {
				break
			}
			for idx, counter := range parts[2:] {
				value, err := strconv.ParseFloat(strings.TrimSpace(counter), 64)
				if err != nil {
					db.log.Error("Compaction entry parsing failed", "err", err)
					merr = err
					continue
				}
				compactions[i%2][idx] += value
			}
		}
		// Update all the requested meters
		if db.diskSizeGauge != nil {
			db.diskSizeGauge.Update(int64(compactions[i%2][0] * 1024 * 1024))
		}
		if db.compTimeMeter != nil {
			db.compTimeMeter.Mark(int64((compactions[i%2][1] - compactions[(i-1)%2][1]) * 1000 * 1000 * 1000))
		}
		if db.compReadMeter != nil {
			db.compReadMeter.Mark(int64((compactions[i%2][2] - compactions[(i-1)%2][2]) * 1024 * 1024))
		}
		if db.compWriteMeter != nil {
			db.compWriteMeter.Mark(int64((compactions[i%2][3] - compactions[(i-1)%2][3]) * 1024 * 1024))
		}
		// Retrieve the write delay statistic
		writedelay, err := db.db.GetProperty("leveldb.writedelay")
		if err != nil {
			db.log.Error("Failed to read database write delay statistic", "err", err)
			merr = err
			continue
		}
		var (
			delayN        int64
			delayDuration string
			duration      time.Duration
			paused        bool
		)
		if n, err := fmt.Sscanf(writedelay, "DelayN:%d Delay:%s Paused:%t", &delayN, &delayDuration, &paused); n != 3 || err != nil {
			db.log.Error("Write delay statistic not found")
			merr = err
			continue
		}
		duration, err = time.ParseDuration(delayDuration)
		if err != nil {
			db.log.Error("Failed to parse delay duration", "err", err)
			merr = err
			continue
		}
		if db.writeDelayNMeter != nil {
			db.writeDelayNMeter.Mark(delayN - delaystats[0])
		}
		if db.writeDelayMeter != nil {
			db.writeDelayMeter.Mark(duration.Nanoseconds() - delaystats[1])
		}
		// If a warning that db is performing compaction has been displayed, any subsequent
		// warnings will be withheld for one minute not to overwhelm the user.
		if paused && delayN-delaystats[0] == 0 && duration.Nanoseconds()-delaystats[1] == 0 &&
			time.Now().After(lastWritePaused.Add(degradationWarnInterval)) {
			db.log.Warn("Database compacting, degraded performance")
			lastWritePaused = time.Now()
		}
		delaystats[0], delaystats[1] = delayN, duration.Nanoseconds()

		// Retrieve the database iostats.
		ioStats, err := db.db.GetProperty("leveldb.iostats")
		if err != nil {
			db.log.Error("Failed to read database iostats", "err", err)
			merr = err
			continue
		}
		var nRead, nWrite float64
		parts := strings.Split(ioStats, " ")
		if len(parts) < 2 {
			db.log.Error("Bad syntax of ioStats", "ioStats", ioStats)
			merr = fmt.Errorf("bad syntax of ioStats %s", ioStats)
			continue
		}
		if n, err := fmt.Sscanf(parts[0], "Read(MB):%f", &nRead); n != 1 || err != nil {
			db.log.Error("Bad syntax of read entry", "entry", parts[0])
			merr = err
			continue
		}
		if n, err := fmt.Sscanf(parts[1], "Write(MB):%f", &nWrite); n != 1 || err != nil {
			db.log.Error("Bad syntax of write entry", "entry", parts[1])
			merr = err
			continue
		}
		if db.diskReadMeter != nil {
			db.diskReadMeter.Mark(int64((nRead - iostats[0]) * 1024 * 1024))
		}
		if db.diskWriteMeter != nil {
			db.diskWriteMeter.Mark(int64((nWrite - iostats[1]) * 1024 * 1024))
		}
		iostats[0], iostats[1] = nRead, nWrite

		compCount, err := db.db.GetProperty("leveldb.compcount")
		if err != nil {
			db.log.Error("Failed to read database iostats", "err", err)
			merr = err
			continue
		}

		var (
			memComp       uint32
			level0Comp    uint32
			nonLevel0Comp uint32
			seekComp      uint32
		)
		if n, err := fmt.Sscanf(compCount, "MemComp:%d Level0Comp:%d NonLevel0Comp:%d SeekComp:%d", &memComp, &level0Comp, &nonLevel0Comp, &seekComp); n != 4 || err != nil {
			db.log.Error("Compaction count statistic not found")
			merr = err
			continue
		}
		db.memCompGauge.Update(int64(memComp))
		db.level0CompGauge.Update(int64(level0Comp))
		db.nonlevel0CompGauge.Update(int64(nonLevel0Comp))
		db.seekCompGauge.Update(int64(seekComp))

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-db.quitChan:
			// Quit requesting, stop hammering the database
		case <-time.After(refresh):
			// Timeout, gather a new set of stats
		}
	}

	if errc == nil {
		errc = <-db.quitChan
	}
	errc <- merr
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type lbatch struct {
	db   *leveldb.DB
	b    *leveldb.Batch
	size int
}
type rbatch struct {
	rdb  *redisdb.DB
	rb   *redisdb.Batch
	size int
}
type mybatch struct {
	lb lbatch
	rb rbatch
}

func (b *mybatch) ValueSize() int {
	return b.lb.size + b.rb.size
}

// Put inserts the given value into the batch for later committing.
func (b *mybatch) Put(key, value []byte) error {
	if isStorageShare(key) {
		b.rb.rb.Put(key, value)
		b.rb.size += len(value)
	} else {
		b.lb.b.Put(key, value)
		b.lb.size += len(value)
	}
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *mybatch) Delete(key []byte) error {
	if isStorageShare(key) {
		b.rb.rb.Delete(key)
		b.rb.size++
	} else {
		b.lb.b.Delete(key)
		b.lb.size++
	}
	return nil
}

// Write flushes any accumulated data to disk.
func (b *mybatch) Write() error {
	b.rb.rb.Write()
	return b.lb.db.Write(b.lb.b, nil)
	//这里写的没有返回err，这个函数只返回了leveldb的err返回值，没有redis的
}

// Reset resets the batch for reuse.
func (b *mybatch) Reset() {
	b.lb.b.Reset()
	b.rb.rb.Reset()
	b.lb.size = 0
	b.rb.size = 0
}

// Replay replays the batch contents.
func (b *mybatch) Replay(w ethdb.KeyValueWriter) error {
	b.rb.rb.Replay(w) //加上rb的
	return b.lb.b.Replay(&replayer{writer: w})
}

// replayer is a small wrapper to implement the correct replay methods.
type replayer struct {
	writer  ethdb.KeyValueWriter
	failure error
}

// Put inserts the given value into the key-value data store.
func (r *replayer) Put(key, value []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Put(key, value)
}

// Delete removes the key from the key-value data store.
func (r *replayer) Delete(key []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Delete(key)
}
