package ssdb

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	key_test       = "6ba7b814-9dad-11d1-80b4-00c04fd430c8"
	value_test     = "1"
	check_duration = time.Minute * 5
	incr_step      = 4
)

var (
	conn_timeout  time.Duration = 15 * time.Second
	read_timeout  time.Duration = 180 * time.Second
	write_timeout time.Duration = 0
	lock          sync.Mutex
)

type SSDBPool struct {
	poolconf   PoolConfig
	idlelist   *list.List
	usedlist   *list.List
	idlecount  int
	usedcount  int
	totalcount int
}
type DBWrapper struct {
	*SSDB
	last_check_time time.Time
}

type PoolConfig struct {
	Host               string
	Port               int
	Initial_conn_count int
	Max_idle_count     int
	Max_conn_count     int
	CheckOnGet         bool
}

func NewPool(pc PoolConfig) (*SSDBPool, error) {
	pool := &SSDBPool{poolconf: pc}
	pool.idlelist = list.New()
	pool.usedlist = list.New()
	for i := 0; i < pc.Initial_conn_count; i++ {
		db, err := Connect(pc.Host, pc.Port, conn_timeout, read_timeout, write_timeout)
		if err != nil {
			return pool, err
		}
		pool.totalcount = pool.totalcount + 1
		pool.idlecount = pool.idlecount + 1
		dbwraper := DBWrapper{db, time.Now()}
		pool.idlelist.PushBack(&dbwraper)
	}
	go func() {

		for {

			t := pool.checkone()
			t = t.Add(check_duration)
			time.Sleep(t.Sub(time.Now()))

		}

	}()

	return pool, nil
}

func (pool *SSDBPool) checkone() time.Time {
	lock.Lock()
	defer lock.Unlock()
	fmt.Printf(" in check\n")
	ele := pool.idlelist.Front()
	if ele != nil {
		db := ele.Value.(*DBWrapper)
		t := db.last_check_time
		err := db.Set(key_test, value_test)
		pool.idlelist.Remove(ele)
		if err == nil {
			pool.idlelist.PushBack(db)
			db.last_check_time=time.Now()
			return t
		} else {
			pool.idlecount = pool.idlecount - 1
			pool.totalcount=pool.totalcount-1
			return db.last_check_time.Add(-1*check_duration)
		}
	}
	return time.Now()
}

func (pool *SSDBPool) GetDB() (*DBWrapper, error) {
	defer lock.Unlock()
	lock.Lock()

	ele := pool.idlelist.Front()
	if ele == nil {
		if pool.totalcount < pool.poolconf.Max_conn_count {
			left := pool.poolconf.Max_conn_count - pool.totalcount
			if left < incr_step {
				pool.incr(left, true)
			} else {
				pool.incr(incr_step, true)
			}
		}

	}
	ele = pool.idlelist.Front()
	if ele == nil {
		return nil, errors.New("can not create more client")
	} else {
		dbwraper := ele.Value.(*DBWrapper)
		err := dbwraper.Set(key_test, value_test)
		if err != nil {
			pool.idlelist.Remove(ele)
			pool.idlecount = pool.idlecount - 1
			pool.totalcount = pool.totalcount - 1

			return nil, errors.New("no idle conn")
		}
		pool.usedlist.PushBack(dbwraper)
		pool.idlecount = pool.idlecount - 1
		pool.usedcount = pool.usedcount + 1
		pool.idlelist.Remove(ele)

		return dbwraper, nil
	}
}

func (pool *SSDBPool) incr(incr_count int, heldlock bool) error {
	if !heldlock {
		lock.Lock()
		defer lock.Unlock()

	}
	for i := 0; i < incr_count; i++ {
		db, err := Connect(pool.poolconf.Host, pool.poolconf.Port, conn_timeout, read_timeout, write_timeout)
		if err != nil {
			return err
		}
		pool.totalcount = pool.totalcount + 1
		pool.idlecount = pool.idlecount + 1
		dbwraper := DBWrapper{db, time.Now()}
		pool.idlelist.PushBack(&dbwraper)
	}
	return nil
}

func (pool *SSDBPool) ReturnDB(db *DBWrapper) error {

	lock.Lock()
	defer lock.Unlock()
	pool.idlelist.PushBack(db)
	pool.idlecount = pool.idlecount + 1
	pool.usedcount = pool.usedcount - 1
	return nil
}

func (pool *SSDBPool) IdleCount() int {
	return pool.idlecount
}
func (pool *SSDBPool) UsedCount() int {
	return pool.usedcount
}
func (pool *SSDBPool) TotalCount() int {
	return pool.totalcount
}
func (pool *SSDBPool) Close() {
	
}
