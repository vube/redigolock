package main

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"github.com/vube/redigolock"
	"time"
)

func runExample(withLock bool) {
	host := "127.0.0.1:6381"
	key := fmt.Sprintf("mylock_%b", withLock)
	res := make(chan bool)

	for i := 0; i < 100; i++ {
		go func() {
			conn, err := redigo.Dial("tcp", host)

			if err != nil {
				fmt.Errorf("redigo.Dial failure due to '%s'", err)
				return
			}

			lock := redigolock.New(conn, key)

			var status bool

			if withLock {
				status, err = lock.Lock()
			} else {
				status, err = true, error(nil)
			}

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

	if withLock {
		fmt.Printf("Non-atomic increment, with lock, incremented to %d (should be 100)\n", v)
	} else {
		fmt.Printf("Non-atomic increment, without lock, incremented to %d (should be 100)\n", v)
	}

	conn.Do("DEL", key)
}

func main() {
	runExample(false)
	runExample(true)
}
