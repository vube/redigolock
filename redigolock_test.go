// Copyright (c) 2013 The Vubeologists. All rights reserved.
// See the license at the root of this project.

package redigolock

import (
	"errors"
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"sync"
	"testing"
	"time"
)

//==============================================================================

const RedisHost = "127.0.0.1:6381"

//==============================================================================

type Redigomock struct {
	FailureDoGetByteString bool
	FailureDoGetString     bool
	FailureDoExec          bool
}

//==============================================================================

// Mock redigo to get full coverage of failure handling
func (m *Redigomock) Do(cmd string, args ...interface{}) (interface{}, error) {
	if m.FailureDoGetByteString {
		return []byte{'a', 'b', 'c'}, nil
	}

	if m.FailureDoGetString {
		return "abc", nil
	}

	if m.FailureDoExec {
		if cmd == "EXEC" {
			return nil, errors.New("mock error")
		}

		return []byte{'1'}, nil
	}

	return nil, errors.New("mock error")
}

func (m *Redigomock) Send(cmd string, args ...interface{}) error {
	return errors.New("mock error")
}

func (m *Redigomock) Close() error {
	return errors.New("mock error")
}

//==============================================================================

// Test a failure with redigo.Do
func Test_FailureDo(t *testing.T) {
	key := "Test_FailureDo"
	conn := Redigomock{}
	conn.FailureDoGetByteString = false
	conn.FailureDoGetString = false
	conn.FailureDoExec = false

	lock := New(Redigoconn(&conn), key, 2000)

	_, err := lock.Lock()

	if err == nil {
		t.Error("redigolock should have errored")
	}
}

// Test a failure with redigo.Do via the function call wrapper
func Test_FailureDoFunc(t *testing.T) {
	key := "Test_IncrementFuncFailure"
	conn := Redigomock{}
	conn.FailureDoGetByteString = false
	conn.FailureDoGetString = false
	conn.FailureDoExec = false

	lock := New(Redigoconn(&conn), key, 2000)

	_, err := lock.LockFunc(func() {

	})

	if err == nil {
		t.Error("redigolock should have errored")
	}
}

// Test a failure with a Redis key existing for the lock with an unknown value
// instead of a TTL
func Test_FailureDoGetByteString(t *testing.T) {
	key := "Test_FailureDoGetString"
	conn := Redigomock{}
	conn.FailureDoGetByteString = true
	conn.FailureDoGetString = false
	conn.FailureDoExec = false

	lock := New(Redigoconn(&conn), key, 2000)

	_, err := lock.Lock()

	if err == nil {
		t.Error("redigolock should have errored")
	}
}

// Test a failure with a Redis key existing for the lock with an unknown value
// type instead of a TTL
func Test_FailureDoGetString(t *testing.T) {
	key := "Test_FailureDoGetString"
	conn := Redigomock{}
	conn.FailureDoGetByteString = false
	conn.FailureDoGetString = true
	conn.FailureDoExec = false

	lock := New(Redigoconn(&conn), key, 2000)

	_, err := lock.Lock()

	if err == nil {
		t.Error("redigolock should have errored")
	}
}

// Test a failure with redigo.Do("EXEC")
func Test_FailureDoExec(t *testing.T) {
	key := "Test_FailureDoGetString"
	conn := Redigomock{}
	conn.FailureDoGetByteString = false
	conn.FailureDoGetString = false
	conn.FailureDoExec = true

	lock := New(Redigoconn(&conn), key, 2000)

	_, err := lock.Lock()

	if err == nil {
		t.Error("redigolock should have errored")
	}
}

//==============================================================================

// Test that general lock acquisition succeeds
func Test_Lock(t *testing.T) {
	key := "Test_Lock"
	conn, err := redigo.Dial("tcp", RedisHost)

	if err != nil {
		t.Error(fmt.Sprintf("redigo.Dial failure [%s]", err))
	}

	lock := New(conn, key)

	status, err := lock.Lock()
	defer lock.UnlockIfLocked()

	if err != nil {
		t.Error(fmt.Sprintf("lock operation failed [%s]", err))
	}

	if !status {
		t.Error("lock acquisition failed")
	}
}

// Test that general lock acquisition succeeds when wrapping a function call
func Test_LockFunc(t *testing.T) {
	key := "Test_LockFunc"
	conn, err := redigo.Dial("tcp", RedisHost)

	if err != nil {
		t.Error(fmt.Sprintf("redigo.Dial failure [%s]", err))
	}

	lock := New(conn, key, 2000)

	status, err := lock.LockFunc(func() {

	})

	if err != nil {
		t.Error(fmt.Sprintf("lock operation failed [%s]", err))
	}

	if !status {
		t.Error("lock acquisition failed")
	}
}

// Test that lock acquisition fails when the lock exists
func Test_BlockedLock(t *testing.T) {
	key := "Test_BlockedLock"
	conn, err := redigo.Dial("tcp", RedisHost)

	if err != nil {
		t.Error(fmt.Sprintf("redigo.Dial failure [%s]", err))
	}

	lock1 := New(conn, key, 2001, 2000)

	status, err := lock1.Lock()
	defer lock1.UnlockIfLocked()

	if err != nil {
		t.Error(fmt.Sprintf("lock operation failed [%s]", err))
	}

	if !status {
		t.Error("lock acquisition failed")
	}

	lock2 := New(conn, key, 1000)

	status, err = lock2.Lock()
	defer lock2.UnlockIfLocked()

	if err != nil {
		t.Error(fmt.Sprintf("lock operation failed [%s]", err))
	}

	if status {
		t.Error("lock acquisition succeeded")
	}
}

// Test that lock acquisition fails when the lock existed and was arbitrarily
// held after being unlocked
func Test_BlockedHold(t *testing.T) {
	key := "Test_BlockedHold"
	conn, err := redigo.Dial("tcp", RedisHost)

	if err != nil {
		t.Error(fmt.Sprintf("redigo.Dial failure [%s]", err))
	}

	lock1 := New(conn, key, DefaultTimeout, DefaultAutoExpire, 2000)

	status, err := lock1.Lock()
	defer lock1.UnlockIfLocked()

	if err != nil {
		t.Error(fmt.Sprintf("lock operation failed [%s]", err))
	}

	if !status {
		t.Error("lock acquisition failed")
	}

	lock1.Unlock()
	lock2 := New(conn, key, 1000)

	status, err = lock2.Lock()
	defer lock2.UnlockIfLocked()

	if err != nil {
		t.Error(fmt.Sprintf("lock operation failed [%s]", err))
	}

	if status {
		t.Error("lock acquisition succeeded")
	}
}

// Test for race conditions when parallelising lock attempts that would cause
// a failure
func Test_MultiLock(t *testing.T) {
	key := "Test_MultiLock"
	res := make(chan bool)

	for i := 0; i < 100; i++ {
		go func() {
			conn, err := redigo.Dial("tcp", RedisHost)

			if err != nil {
				t.Error(fmt.Sprintf("redigo.Dial failure [%s]", err))
			}

			lock := New(conn, key, 2000, DefaultAutoExpire, DefaultHold, 5)

			status, _ := lock.Lock()
			defer lock.UnlockIfLocked()

			res <- status
		}()
	}

	for i := 1; i <= 100; i++ {
		if !<-res {
			t.Error("lock acquisition failed")
		}
	}
}

// Test that our locks are actually preventing race conditions by doing a
// non-atomic Redis operation surrounded by a lock
func Test_Increment(t *testing.T) {
	key := "Test_Increment"
	wg := new(sync.WaitGroup)

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			conn, err := redigo.Dial("tcp", RedisHost)

			if err != nil {
				t.Errorf("redigo.Dial failure due to '%s'", err)
				return
			}

			lock := New(conn, key, 2000, DefaultAutoExpire, DefaultHold, 5)

			status, err := lock.Lock()

			if status {
				// arbitrary distributed non-atomic operation
				v, _ := redigo.Int(conn.Do("GET", key))
				v++
				time.Sleep(100000)
				conn.Do("SET", key, v)
			} else {
				if err != nil {
					t.Errorf("lock operation failure due to '%s", err)
				} else {
					t.Error("timed out during lock contention")
				}
			}

			lock.UnlockIfLocked()
		}()
	}

	wg.Wait()

	conn, err := redigo.Dial("tcp", RedisHost)

	if err != nil {
		t.Errorf("redigo.Dial failure due to '%s'", err)
	}

	v, _ := redigo.Int(conn.Do("GET", key))

	if v != 100 {
		t.Error("increment miscalculation")
	}

	conn.Do("DEL", key)
}
