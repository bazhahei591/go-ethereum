package redisdb

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

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

func (d *DB) Set(k []byte, v []byte) error {
	_, err := d.c.Do("SET", k, v)
	return err
}

func demo() {
	c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialDatabase(1))
	if err != nil {
		fmt.Println("connect redis error :", err)
		return
	}
	defer c.Close()

	v, err := c.Do("SET", "name", "red")
	if err != nil {
		fmt.Println("redis set failed:", err)
		return
	}
	fmt.Println(v)
	v, err = redis.String(c.Do("GET", "name"))
	if err != nil {
		fmt.Println("redis get failed:", err)
		return
	}
	fmt.Println("getName:", v)

	// c.Do("lpush", "redlist", "qqq")
	// c.Do("lpush", "redlist", "www")
	// c.Do("lpush", "redlist", "eee")

	values, _ := redis.Values(c.Do("lrange", "redlist", "0", "100"))

	// for _, v := range values {
	// 	fmt.Println(string(v.([]byte)))
	// }

	// 或者
	var v1 string
	redis.Scan(values, &v1)
	fmt.Println(v1)

}
