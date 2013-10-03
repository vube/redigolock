// Copyright (c) 2013 The Vubeologists. All rights reserved
// See the license at the root of this project

package redigolock

import (
	"bytes"
	"time"
	"fmt"
	"strconv"
)

//==============================================================================

const (
	DEFAULT_AUTO_EXPIRE = 30000 // 30 second timeout on all locks
	DEFAULT_HOLD = 0 // Do not artifically hold locks after they're unlocked
	DEFAULT_TICK = 100 // .1 second retry interval
)

//==============================================================================

// Allow for mocking redigo.Conn
type Redigoconn interface {
	Do(cmd string, args ...interface{}) (interface{}, error)
	Send(cmd string, args ...interface{}) error
	Close() error
}

type Redigolock struct {
	// The connection to Redis
	Conn Redigoconn

	// The base key we should work with
	Key string

	// How long before giving up on the lock. In milliseconds
	Timeout int64

	// How long to expire the lock if it's never unlocked. In milliseconds
	AutoExpire int64

	// How long to have the lock linger after it's unlocked. In milliseconds
	Hold int64

	// The retry interval before giving up on the lock. In milliseconds
	Tick time.Duration

	// Tracks whether or not we obtained the lock
	locked bool
}

//==============================================================================

// Returns the full key used for the Redis lock operation
func (r *Redigolock) generateLockKey() (string) {
	var buf bytes.Buffer
	buf.WriteString("L:{")
	buf.WriteString(r.Key)
	buf.WriteString("}")
	return buf.String()
}

//==============================================================================

// Returns a new Redigolock struct which is used to hold the configurations for
// the behavior of the lock.
func New(conn Redigoconn, key string, timeout int64) *Redigolock {
	r := new(Redigolock)

	r.Conn = conn;
	r.Key = key
	r.Timeout = timeout
	r.AutoExpire = DEFAULT_AUTO_EXPIRE
	r.Hold = DEFAULT_HOLD
	r.Tick = DEFAULT_TICK
	r.locked = false

	return r
}

// Returns true if the lock operation succeeded and false if not, with an error
// if the operation failed due to any other reason than a normal timeout during
// an attempt to acquire the lock.
//
// A lock attempt will fail under the following conditions:
//   * The Redis connection fails
//   * The Redis command fails (ie, nutcracker or another proxy service prevents
//     our command set from being supported)
//   * The key exists and contains an unexpected, non-integer value
//   * The lock key exists and the value is a time greater than the current
//     time, and our timeout is exceeded
//   * The lock is obtained with another transaction and our timeout is exceeded
//   * The key is modified in some way which invalidates our lock transaction
//     and our timeout is exceeded
func (r *Redigolock) Lock() (bool, error) {
	status := false
	r.locked = status
	timeout := time.Now().UnixNano() / 1000000 + r.Timeout

	for time.Now().UnixNano() / 1000000 < timeout {
		lkey := r.generateLockKey()

		r.Conn.Send("WATCH", lkey)
		res, err := r.Conn.Do("GET", lkey)

		if err != nil {
			return status, err
		}

		var key_expire int64 = 0

		// Look at the actual value of the lock to get the TTL rather than just
		// checking for the existence of the lock

		if res != nil {
			switch v := res.(type) {
				case []byte:
					key_expire, err = strconv.ParseInt(string(v), 10, 64)

					if err != nil {
						return status, err
					}
				default:
					return status, fmt.Errorf("Unexpected response on GET lock")
			}
		}

		if key_expire < time.Now().UnixNano() / 1000000 {
			r.Conn.Send("MULTI")
			r.Conn.Send("SET", lkey, time.Now().UnixNano() / 1000000 + r.AutoExpire)
			r.Conn.Send("PEXPIRE", lkey, r.AutoExpire + r.Hold)
			res, err := r.Conn.Do("EXEC")

			if err != nil {
				return status, err
			}

			// Someone just took our lock
			if res == nil {
				time.Sleep(r.Tick * time.Millisecond)
				continue
			}

			status = true
			r.locked = status
			return status, nil
		}

		time.Sleep(r.Tick * time.Millisecond)
	}

	return status, nil
}

// Allow for a routine to be surrounded by a lock.  Wraps Lock/UnlockIfLocked.
func (r *Redigolock) LockFunc(call func()) (status bool, err error) {
  status, err = r.Lock()
  defer r.UnlockIfLocked()
  if ! status {
  	return
  }
  call()
  return
}

// Unlock a lock
func (r *Redigolock) Unlock() (error) {
	var err error
	if r.Hold > 0 {
		_, err = r.Conn.Do("PEXPIRE", r.generateLockKey(), r.Hold)
	} else {
		_, err = r.Conn.Do("DEL", r.generateLockKey())
	}
	return err
}

// Unlock a lock only if we locked, so you can defer the unlock and not
// break someone else's lock
func (r *Redigolock) UnlockIfLocked() (error) {
	if r.locked {
		return r.Unlock()
	}

	return nil
}
