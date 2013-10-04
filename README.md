Redigolock
==========

A distributed optimistic locking manager implemented in
[Go](http://golang.org/), using the [Redis](http://redis.io/) data store with
the [Redigo](https://github.com/garyburd/redigo/) client.

Installation
------------

Install Redigolock using the "go get" command:

    go get github.com/vube/redigolock

The Go distribution, a connection to a Redis server capable of keyless commands
(ie, not behind nutcracker) and [Redigo](https://github.com/garyburd/redigo/)
are Redigolock's dependencies.

Example
-------

```go
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

			lock := redigolock.New(conn, key)

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
```

Output:
```
redigolock$ time go run example/example.go
key = 100

real	0m23.117s
user	0m0.292s
sys	0m1.020s
```

License
-------

Redigolock is available under the [MIT License](https://github.com/vube/redigolock/blob/master/LICENSE).
