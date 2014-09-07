package ssdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var (
	conn_timeout  time.Duration = 15 * time.Second
	read_timeout  time.Duration = 180 * time.Second
	write_timeout time.Duration = 0
)

func TestGetSet(t *testing.T) {
	db, err := Connect("jiecao-tucao", 8888, conn_timeout, read_timeout, write_timeout)
	if err != nil {
		fmt.Printf("connect to server failed:\n%v", err)
	}
	db.Set("key1", "value1")
	ex, _ := db.Exists("key1")
	assert.True(t, ex, "set failed")
	value, _ := db.Get("key1")
	assert.Equal(t, value, "value1", "key1 not equal value1")
	db.Del("key1")
	ex, _ = db.Exists("key1")
	assert.False(t, ex, "del failed")
	for i := 1; i < 5; i++ {
		db.Set("key"+fmt.Sprintf("%d", i), "value"+fmt.Sprintf("%d", i))
	}
	keys, _ := db.Keys("key0", "key4", 10)
	assert.Equal(t, 4, len(keys), "keys test failed")
	m, _ := db.Scan("key0", "key4", 10)
	assert.Equal(t, 4, len(m), "scan error")
	m, _ = db.RScan("key5", "key1", 10)
	assert.Equal(t, len(m), 4)
	db.Del("lxy")
	db.Incr("lxy", 100)
	v, _ := db.Get("lxy")
	assert.Equal(t, "100", v, "incr error")

	_, err = db.MultiSet([]string{"k1", "v1", "k2", "v2", "k3", "v3"})
	if err != nil {
		fmt.Printf("%v,\n")
	}
	m, _ = db.MultiGet([]string{"k1", "k2", "k3"})
	assert.Equal(t, 3, len(m), "multiset or multget failed")
	db.MultiDel([]string{"k1", "k2", "k3"})
	m, _ = db.Scan("k0", "k4", 10)
	assert.Equal(t, 0, len(m), "multidel failed")
	db.Close()

}

func TestZset(t *testing.T) {
	db, err := Connect("jiecao-tucao", 8888, conn_timeout, read_timeout, write_timeout)
	if err != nil {
		fmt.Printf("connect to server failed:\n%v", err)
	}

	db.ZSet("set1", "key1", 100)
	score, _ := db.ZGet("set1", "key1")
	assert.Equal(t, 100, score, "zset failed")

	db.ZIncr("set1", "key1", 100)
	score, _ = db.ZGet("set1", "key1")
	assert.Equal(t, 200, score, "zincr failed")

	size, _ := db.ZSize("set1")
	assert.Equal(t, 1, size, "zsie failed")
	db.ZDel("set1", "key1")
	score, err = db.ZGet("set1", "key1")
	assert.NotNil(t, err, "zget failed")

	var i int64
	for i = 10; i < 15; i++ {
		db.ZSet("set2", "k"+fmt.Sprintf("%d", i), i)
	}
	m, _ := db.ZScan("set2", "k", int64(10), int64(15), 100)
	assert.Equal(t, len(m), 5, "zscan failed")

	sets, _ := db.ZList("set", "set2", 10)
	assert.Equal(t, 1, len(sets), "zlist failed")
	db.ZClear("set2")
	size, _ = db.ZSize("set2")
	assert.Equal(t, 0, size, "zclear failed")

	for i = 50; i < 55; i++ {
		db.ZSet("set3", "k"+fmt.Sprintf("%d", i), i)
	}
	count, er := db.ZCount("set3", 50, 55)
	if er != nil {
		fmt.Printf("%v\n", er)
	}
	assert.Equal(t, 5, count, "zcount failed")

	ex, err := db.ZExists("set3", "k50")
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	assert.True(t, ex, "zexists failed")

	ks, _ := db.ZKeys("set3", "k49", 50, 54, 30)
	assert.Equal(t, len(ks), 5, "zkeys failed")
	mp := make(map[string]int64)
	mp["mk1"] = int64(12)
	mp["mk2"] = int64(13)
	err = db.MultiZset("set4", mp)
	if err != nil {
		fmt.Printf("%v", err)
	}
	assert.Nil(t, err, "multizset failed")
	mp, _ = db.MultiZGet("set4", []string{"mk1", "mk2"})
	assert.Equal(t, 2, len(mp), "zmultget failed")
	db.Close()

}

func TestHash(t *testing.T) {
	db, err := Connect("jiecao-tucao", 8888, conn_timeout, read_timeout, write_timeout)
	if err != nil {
		fmt.Printf("connect to server failed:\n%v", err)
	}
	db.HSet("hash1", "lxy", "tiger")
	value, _ := db.HGet("hash1", "lxy")
	assert.Equal(t, value, "tiger", "hset or hget failed")
	exist, _ := db.HExists("hash1", "lxy")
	assert.True(t, exist, "hexits failed")
	size, _ := db.HSize("hash1")
	assert.Equal(t, 1, size, "hsize failed")
	db.HDel("hash4", "lxy")
	db.HIncr("hash4", "lxy", 1000)
	count, _ := db.HIncr("hash4", "lxy", 0)
	assert.Equal(t, 1000, count, "hincr failed")
	value, _ = db.HGet("hash4", "lxy")
	assert.Equal(t, value, "1000", "hincr failed")
	db.HSet("hash2", "lxy", "tiger")
	db.HSet("hash3", "lxy", "tiger")
	names, _ := db.HList("hash1", "hash3", 5)
	assert.Equal(t, len(names), 2, "hlist failed")
	names, _ = db.HRlist("hash3", "hash1", 5)
	assert.Equal(t, len(names), 2, "hrlist failed")

	for i := 0; i < 5; i++ {
		db.HSet("hash5", "lxy"+strconv.Itoa(i), "tiger")
	}
	names, _ = db.HKeys("hash5", "lxy0", "lxy4", 10)
	assert.Equal(t, 4, len(names), "hkeys failed")
	m, _ := db.HGetAll("hash5")
	assert.Equal(t, 5, len(m), "hgetall failed")
	m, _ = db.HScan("hash5", "lxy0", "lxy4", 100)
	assert.Equal(t, 4, len(m), "hscan failed")
	m, _ = db.HRscan("hash5", "lxy4", "lxy0", 50)
	assert.Equal(t, 4, len(m), "hrscan failed")
	db.HClear("hash5")
	size, _ = db.HSize("hash5")
	assert.Equal(t, 0, size, "hclear failed")

	db.MultiHSet("hash6", []string{"lxy0", "v1", "lxy2", "v2", "lxy3", "v3", "lxy4", "v4"})
	size, _ = db.HSize("hash6")
	assert.Equal(t, size, 4, "hmultiset failed")
	m, _ = db.MultiHGet("hash6", []string{"lxy0", "lxy3"})
	assert.Equal(t, len(m), 2, "hmultiget failed")
	_, err = db.MultiHDel("hash6", []string{"lxy2", "lxy3"})
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	size, _ = db.HSize("hash6")
	assert.Equal(t, size, 2, "multihdel failed")
	db.HClear("hash6")

	db.Close()
}

func TestQ(t *testing.T) {
	db, err := Connect("jiecao-tucao", 8888, conn_timeout, read_timeout, write_timeout)
	if err != nil {
		fmt.Printf("connect to server failed:\n%v", err)
	}
	db.QClear("list1")
	db.QPushFront("list1", "lxy0")
	db.QPushBack("list1", "lxy1")
	value, _ := db.QFront("list1")
	assert.Equal(t, value, "lxy0", "q_pushfront failed")
	value, _ = db.QBack("list1")
	assert.Equal(t, value, "lxy1", "q_pushback failed")
	value, _ = db.QGet("list1", 1)
	assert.Equal(t, "lxy1", value, "qget failed")
	db.QPushBack("list1", "lxy3")
	db.QPushBack("list1", "tiger")
	values, _ := db.QSlice("list1", 1, -1)
	assert.Equal(t, len(values), 3, "qslice failed")

	size, _ := db.QSize("list1")
	assert.Equal(t, 4, size, "qsize failed")
	db.QPushFront("list2", "lxy0")
	db.QPushBack("list3", "lxy0")
	names, _ := db.QList("list1", "list3", 5)
	assert.Equal(t, len(names), 2, "qlist failed")
	names, _ = db.QRlist("list3", "list1", 5)
	assert.Equal(t, len(names), 2, "qrlist failed")
	db.QClear("list1")
	db.QClear("list2")
	db.QClear("list3")

	db.Close()
}
