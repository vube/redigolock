package main

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"github.com/vube/redigolock"
	"time"
)

func main() {
	host := "127.0.0.1:6381"
	key := "mylock"
	res := make(chan bool)

	for i := 0; i < 100; i++ {
		go func() {
			conn, err := redigo.Dial("tcp", host)

			if err != nil {
				fmt.Errorf("redigo.Dial failure due to '%s'", err)
				return
			}

			lock := redigolock.New(conn, key, 30000)

			status, err := lock.Lock()

			if status {
				// arbitrary distributed non-atomic operation
				v, _ := redigo.Int(conn.Do("GET", key))
				v++
				time.Sleep(100 * time.Millisecond)
				conn.Do("SET", key, v)
			} else {
				if err != nil {
					fmt.Errorf("lock operation failure due to '%s", err)
				} else {
					fmt.Errorf("timed out during lock contention")
				}
			}

			lock.UnlockIfLocked()
			res <- status
		}()
	}

	for i := 0; i < 100; i++ {
		<-res
	}

	conn, err := redigo.Dial("tcp", host)

	if err != nil {
		fmt.Errorf("redigo.Dial failure due to '%s'", err)
		return
	}

	v, _ := redigo.Int(conn.Do("GET", key))
	fmt.Printf("key = %d\n", v)
	conn.Do("DEL", key)
}
