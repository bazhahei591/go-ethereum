package redisdb

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

type BatchItem struct {
	command string
	key []byte
	value [] byte
}

type Batch struct{
	l  []BatchItem
	c redis.Conn
} 

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
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
		if item.command == "SET"{
			r.Put(item.key, item.value)
		}
		if item.command == "DEL"{
			r.Delete(item.key)
		}
	}
	return nil
}

func NewBatch(c redis.Conn) *Batch{
	b:=Batch{c: c}
	b.l = make([]BatchItem)
	return &b
}

func (b *Batch)Put(key []byte, value []byte) {
	append(b.l , BatchItem{command: "SET", key: key, value: value})
}

func (b *Batch) Write (){
	for _, it := range b.l {
		b.c.Do(it.command, it.key, it.value)
	}
	// b.l = //empty
}

type DB struct {
	c redis.Conn
}

func New() *DB {
	c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialDatabase(0))
	if err != nil {
		fmt.Println("connect redis error :", err)
		return nil
	}
	return &DB{c}
}

func (d *DB) Close() error {
	return d.c.Close()
}

func (d *DB) Has(key []byte) (bool, error) {
	v, err := d.Get(key)
	return v != nil, err
}

func (d *DB) Set(key []byte, value []byte) error {
	_, err := d.c.Do("SET", key, value)
	if err != nil {
		fmt.Println("redis set failed:", err)
		return err
	}
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	value, err := redis.Bytes(d.c.Do("GET", key))
	if err != nil {
		fmt.Println("redis get failed:", err)
		return nil, err
	}
	return value, nil
}

func (d *DB) Del(key []byte) error {
	_, err := d.c.Do("DEL", key)
	if err != nil {
		fmt.Println("redis delete failed:", err)
		return err
	}
	return err
}

// func (d *DB) Compact (){
// 	//TODO
// }

func (d *DB) 