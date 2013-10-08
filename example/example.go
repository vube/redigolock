// Copyright (c) 2013 The Vubeologists. All rights reserved.
// See the license at the root of this project.

package main

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"github.com/vube/redigolock"
	"sync"
	"time"
)

func runExample(withLock bool) {
	host := "127.0.0.1:6381"
	key := fmt.Sprintf("mylock_%t", withLock)
	wg := new(sync.WaitGroup)

	conn, err := redigo.Dial("tcp", host)

	if err != nil {
		fmt.Errorf("redigo.Dial failure due to '%s'", err)
		return
	}

	conn.Do("DEL", key)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}

	wg.Wait()

	v, _ := redigo.Int(conn.Do("GET", key))

	if withLock {
		fmt.Printf("Non-atomic increment, with lock, incremented to %d (should be 10)\n", v)
	} else {
		fmt.Printf("Non-atomic increment, without lock, incremented to %d (should be 10)\n", v)
	}

	conn.Do("DEL", key)
}

func main() {
	for {
		runExample(false)
		runExample(true)
		fmt.Println()
	}
}
