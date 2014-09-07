package ssdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

type Error string

type Conn interface {
	//close the tcp connection
	Close() error
	//return non-nil value if the connection is broken
	Err() error
	//sends a command to the server and returns the received response
	Do(cmd string, args []interface{}) (rsp []bytes.Buffer, err error)
	//sends a command to the server
	Send(cmd string, args []interface{}) error
	//flushes the output buffer to the server
	Flush() error
	//receives a single reply from server
	Receive() (rsp []bytes.Buffer, err error)
}

type Client interface {
	Set(key string, value string) error
	Get(key string) (result string, err error)
	Del(key string) (bool, error)
	Exists(key string) (bool, error)
	//  key_start<key<=key_end
	Keys(key_start, key_end string, limit int) ([]string, error)
	// key_start<key<=key_end
	Scan(key_start, key_end string, limit int) (map[string]string, error)
	// key_end<=key<key_start
	RScan(key_start, key_end string, limit int) (map[string]string, error)
	Incr(key string, by int64) (value int64, err error)
	MultiSet(kvs []string) (bool, error)
	MultiGet(keys []string) (map[string]string, error)
	MultiDel(keys []string) (bool, error)

	ZSet(setname, key string, score int64) error
	ZGet(setname, key string) (int64, error)
	ZIncr(setname, key string, by int64) (value int64, err error)
	ZDel(sentname, key string) error
	ZSize(setname string) (int64, error)
	ZScan(setname, keystart string, score_start, score_end int64, limit int) (map[string]int64, error)
	// name_start<name<=name_end
	ZList(name_start, name_end string, limit int) ([]string, error)
	ZClear(setname string) error
	ZCount(setname string, score_start, score_end int64) (int, error)
	ZExists(setname, key string) (bool, error)
	// key_start<key   score_start<=score<=score_end
	ZKeys(setname, key_start string, score_start, score_end int64, limit int) ([]string, error)
	MultiZGet(setname string, keys []string) (map[string]int64, error)
	MultiZset(setname string, kvs map[string]int64) error

	HSet(name, key, value string) (bool, error)
	HGet(name, key string) (string, error)
	HDel(name, key string) (bool, error)
	HIncr(name, key string, by int64) (int64, error)
	HExists(name, key string) (bool, error)
	HSize(name string) (int64, error)
	//      name_start<name<=name_end
	HList(name_start, name_end string, limit int) ([]string, error)
	HRlist(name_start, name_end string, limit int) ([]string, error)
	HKeys(name, key_start, key_end string, limit int) ([]string, error)
	HGetAll(name string) (map[string]string, error)
	HScan(name, key_start, key_end string, limit int) (map[string]string, error)
	HRscan(name, key_start, key_end string, limit int) (map[string]string, error)
	HClear(name string) (bool, error)
	MultiHSet(name string, kvs []string) (bool, error)
	MultiHGet(name string, keys []string) (map[string]string, error)
	MultiHDel(name string, keys []string) (bool, error)

	QPushFront(name, value string) (int64, error)
	QPushBack(name, value string) (int64, error)
	QPopFront(name string) (string, error)
	QPopBack(name string) (string, error)
	QSize(name string) (int64, error)
	QList(name_start, name_end string, limit int) ([]string, error)
	QRlist(name_start, name_end string, limit int) ([]string, error)
	QClear(name string) (bool, error)
	QFront(name string) (string, error)
	QBack(name string) (string, error)
	QGet(name string, index int64) (string, error)
	//  begin< index <=end
	QSlice(name string, begin, end int64) ([]string, error)
}

func connect(host string, port int, conntimeout, readtimeout, writetimeout time.Duration) (Conn, error) {
	c := &conn{}
	connection, er := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), conntimeout)
	if er != nil {
		return nil, er
	}
	c.client = connection
	c.writer = bufio.NewWriter(connection)
	c.reader = bufio.NewReader(connection)
	return c, nil
}

func Connect(host string, port int, conntimeout, readtimeout, writetimeout time.Duration) (*SSDB, error) {
	conn, err := connect(host, port, conntimeout, readtimeout, writetimeout)
	return &SSDB{conn: conn}, err
}

type SSDB struct {
	conn Conn
}

func (db *SSDB) Close() {
	_ = db.conn.Close()
}

func (db *SSDB) Set(key string, value string) error {
	resp, err := db.conn.Do("set", []interface{}{key, value})
	if resp[0].String() != "ok" {
		fmt.Println("set failed")
	}
	return err
}

func (db *SSDB) MultiSet(kvs []string) (bool, error) {
	resp, err := db.conn.Do("multi_set", []interface{}{kvs})
	if err != nil {
		return false, err
	}
	return resp[0].String() == "ok", nil
}

func (db *SSDB) Get(key string) (string, error) {
	resp, err := db.conn.Do("get", []interface{}{key})
	if resp[0].String() != "ok" {
		fmt.Println("set failed")
	}
	return resp[1].String(), err
}

func (db *SSDB) MultiGet(keys []string) (map[string]string, error) {

	resp, err := db.conn.Do("multi_get", []interface{}{keys})
	if err != nil {
		return nil, err
	}

	return StringMap(resp)
}
func (db *SSDB) MultiDel(keys []string) (bool, error) {

	resp, err := db.conn.Do("multi_del", []interface{}{keys})
	if err != nil {
		return false, err
	}
	if resp[0].String() != "ok" {
		return false, errors.New(resp[0].String())
	}

	return true, nil
}

func (db *SSDB) Scan(key_start, key_end string, limit int) (map[string]string, error) {

	resp, err := db.conn.Do("scan", []interface{}{key_start, key_end, limit})
	if err != nil {
		return nil, err
	}
	return StringMap(resp)
}

func (db *SSDB) RScan(key_start, key_end string, limit int) (map[string]string, error) {

	resp, err := db.conn.Do("rscan", []interface{}{key_start, key_end, limit})
	if err != nil {
		return nil, err
	}
	return StringMap(resp)

}

func (db *SSDB) Del(key string) (bool, error) {
	resp, err := db.conn.Do("del", []interface{}{key})
	if err != nil {
		return false, err
	}
	return resp[0].String() == "ok", nil

}

func (db *SSDB) Keys(key_start, key_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("keys", []interface{}{key_start, key_end, limit})
	if err != nil {
		return nil, err
	}
	return StringArray(resp)
}

func (db *SSDB) Exists(key string) (bool, error) {

	resp, err := db.conn.Do("exists", []interface{}{key})
	if err != nil {
		return false, err
	}

	return BoolValue(resp)

}

func (db *SSDB) Incr(key string, by int64) (int64, error) {
	resp, err := db.conn.Do("incr", []interface{}{key, by})
	if err != nil {
		return 0, err
	}
	return Int64(resp)
}

func (db *SSDB) ZSet(setname, key string, score int64) error {
	resp, err := db.conn.Do("zset", []interface{}{setname, key, score})
	if resp[0].String() != "ok" {
		return errors.New(resp[0].String())
	}
	return err
}

func (db *SSDB) ZGet(setname, key string) (int64, error) {

	resp, err := db.conn.Do("zget", []interface{}{setname, key})
	if resp[0].String() != "ok" {
		return 0, errors.New(resp[0].String())
	}
	if err != nil {
		return -1, err
	}
	return strconv.ParseInt(resp[1].String(), 10, 64)
}

func (db *SSDB) ZIncr(setname, key string, by int64) (int64, error) {
	resp, err := db.conn.Do("zincr", []interface{}{setname, key, by})

	if err != nil {
		return 0, err
	}

	if resp[0].String() != "ok" {
		return 0, errors.New(resp[0].String())
	}

	return strconv.ParseInt(resp[1].String(), 10, 64)
}

func (db *SSDB) ZDel(setname, key string) error {
	resp, err := db.conn.Do("zdel", []interface{}{setname, key})
	if err != nil {
		return err
	}
	if resp[0].String() != "ok" {
		return errors.New(resp[0].String())
	}
	return nil
}

func (db *SSDB) ZSize(setname string) (int64, error) {
	resp, err := db.conn.Do("zsize", []interface{}{setname})
	if err != nil {
		return 0, err
	}
	if resp[0].String() != "ok" {
		return 0, errors.New(resp[0].String())

	}

	return strconv.ParseInt(resp[1].String(), 10, 64)
}

func (db *SSDB) ZScan(setname, key_start string, score_start, score_end int64, limit int) (map[string]int64, error) {

	resp, err := db.conn.Do("zscan", []interface{}{setname, key_start, score_start, score_end, limit})
	if err != nil {
		return nil, err
	}

	if resp[0].String() != "ok" {
		return nil, errors.New(resp[0].String())
	}

	res := make(map[string]int64)
	var key string
	key = ""
	for _, buf := range resp[1:] {
		if key == "" {
			key = buf.String()
		} else {
			res[key], _ = strconv.ParseInt(buf.String(), 10, 64)
			key = ""
		}
	}

	return res, nil
}

func (db *SSDB) ZClear(setname string) error {
	resp, err := db.conn.Do("zclear", []interface{}{setname})
	if err != nil {
		return err
	}
	if resp[0].String() != "ok" {
		return errors.New(resp[0].String())
	}

	return nil
}

func (db *SSDB) ZList(name_start, name_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("zlist", []interface{}{name_start, name_end, limit})
	if err != nil {
		return nil, err
	}
	if resp[0].String() != "ok" {
		return nil, errors.New(resp[0].String())
	}
	var res []string
	for _, buf := range resp[1:] {
		res = append(res, buf.String())
	}

	return res, nil
}
func (db *SSDB) ZCount(setname string, score_start, score_end int64) (int, error) {
	resp, err := db.conn.Do("zcount", []interface{}{setname, score_start, score_end})
	if err != nil {
		return 0, err
	}

	return IntValue(resp)
}
func (db *SSDB) ZExists(setname, key string) (bool, error) {

	resp, err := db.conn.Do("zexists", []interface{}{setname, key})
	if err != nil {
		return false, err
	}
	if resp[0].String() != "ok" {
		return false, errors.New(resp[0].String())
	}
	return BoolValue(resp)
}

func (db *SSDB) ZKeys(setname, key_start string, score_start, score_end int64, limit int) ([]string, error) {

	resp, err := db.conn.Do("zkeys", []interface{}{setname, key_start, score_start, score_end, limit})
	if err != nil {
		return nil, err
	}
	if resp[0].String() != "ok" {
		return nil, errors.New(resp[0].String())
	}
	var res []string
	for _, b := range resp[1:] {
		res = append(res, b.String())
	}
	return res, nil
}

func (db *SSDB) MultiZGet(setname string, keys []string) (map[string]int64, error) {
	var array []interface{}
	array = append(array, setname)
	for _, key := range keys {
		array = append(array, key)
	}
	resp, err := db.conn.Do("multi_zget", array)
	if err != nil {
		return nil, err
	}
	if resp[0].String() != "ok" {
		return nil, errors.New(resp[0].String())
	}

	res := make(map[string]int64)
	key := ""
	for _, buf := range resp[1:] {
		if key == "" {
			key = buf.String()
		} else {
			res[key], _ = strconv.ParseInt(buf.String(), 10, 64)
			key = ""
		}
	}
	return res, nil
}
func (db *SSDB) MultiZset(setname string, kvs map[string]int64) error {
	var kva []interface{}
	kva = append(kva, setname)
	for k, v := range kvs {
		kva = append(kva, k)
		kva = append(kva, v)
	}
	resp, err := db.conn.Do("multi_zset", kva)
	if err != nil {
		return err
	}
	if "ok" != resp[0].String() {
		return errors.New(resp[0].String())
	}
	return nil
}

func (db *SSDB) HSet(name, key, value string) (bool, error) {

	resp, err := db.conn.Do("hset", []interface{}{name, key, value})
	if err != nil {
		return false, err
	}

	if resp[0].String() != "ok" {
		return false, errors.New(resp[0].String())
	}
	return true, nil
}

func (db *SSDB) HGet(name, key string) (string, error) {
	resp, err := db.conn.Do("hget", []interface{}{name, key})
	if err != nil {
		return "", err
	}
	return StringValue(resp)
}

func (db *SSDB) HDel(name, key string) (bool, error) {

	resp, err := db.conn.Do("hdel", []interface{}{name, key})
	if err != nil {
		return false, err
	}
	if resp[0].String() != "ok" {
		return false, errors.New(resp[0].String())
	}
	return true, nil
}

func (db *SSDB) HIncr(name, key string, by int64) (int64, error) {
	resp, err := db.conn.Do("hincr", []interface{}{name, key, by})
	if err != nil {
		return 0, err
	}

	return Int64(resp)
}

func (db *SSDB) HExists(name, key string) (bool, error) {
	resp, err := db.conn.Do("hexists", []interface{}{name, key})
	if err != nil {
		return false, err
	}

	return BoolValue(resp)

}

func (db *SSDB) HSize(name string) (int64, error) {
	resp, err := db.conn.Do("hsize", []interface{}{name})
	if err != nil {
		return 0, err
	}
	if resp[0].String() != "ok" {
		return 0, errors.New(resp[0].String())
	}
	return Int64(resp)
}

func (db *SSDB) HList(name_start, name_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("hlist", []interface{}{name_start, name_end, limit})
	if err != nil {
		return nil, err
	}

	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringArray(resp)
}

func (db *SSDB) HRlist(name_start, name_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("hrlist", []interface{}{name_start, name_end, limit})
	if err != nil {
		return nil, err
	}

	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringArray(resp)
}

func (db *SSDB) HKeys(name, key_start, key_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("hkeys", []interface{}{name, key_start, key_end, limit})
	if err != nil {
		return nil, err
	}

	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringArray(resp)
}

func (db *SSDB) HGetAll(name string) (map[string]string, error) {
	resp, err := db.conn.Do("hgetall", []interface{}{name})
	if err != nil {
		return nil, err
	}

	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringMap(resp)
}

func (db *SSDB) HScan(name, key_start, key_end string, limit int) (map[string]string, error) {
	resp, err := db.conn.Do("hscan", []interface{}{name, key_start, key_end, limit})
	if err != nil {
		return nil, err
	}

	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringMap(resp)
}

func (db *SSDB) HRscan(name, key_start, key_end string, limit int) (map[string]string, error) {
	resp, err := db.conn.Do("hrscan", []interface{}{name, key_start, key_end, limit})
	if err != nil {
		return nil, err
	}

	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringMap(resp)
}

func (db *SSDB) HClear(name string) (bool, error) {
	resp, err := db.conn.Do("hclear", []interface{}{name})
	if err != nil {
		return false, err
	}
	if "ok" != resp[0].String() {
		return false, errors.New(resp[0].String())
	}
	return true, nil
}

func (db *SSDB) MultiHSet(name string, kvs []string) (bool, error) {
	resp, err := db.conn.Do("multi_hset", []interface{}{name, kvs})
	if err != nil {
		return false, err
	}
	if resp[0].String() != "ok" {
		return false, errors.New(resp[0].String())
	}
	return resp[1].String() == "1", nil
}

func (db *SSDB) MultiHGet(name string, keys []string) (map[string]string, error) {
	resp, err := db.conn.Do("multi_hget", []interface{}{name, keys})
	if err != nil {
		return nil, err
	}
	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}

	return StringMap(resp)
}

func (db *SSDB) MultiHDel(name string, keys []string) (bool, error) {
	resp, err := db.conn.Do("multi_hdel", []interface{}{name, keys})
	if err != nil {
		return false, err
	}
	return BoolValue(resp)
}

func (db *SSDB) QPushFront(name, value string) (int64, error) {
	resp, err := db.conn.Do("qpush_front", []interface{}{name, value})
	if err != nil {
		fmt.Println("push front error")
		return 0, err
	}
	if "ok" != resp[0].String() {
		return 0, errors.New(resp[0].String())
	}
	res, er := Int64(resp)
	return res, er
}
func (db *SSDB) QPushBack(name, value string) (int64, error) {
	resp, err := db.conn.Do("qpush_back", []interface{}{name, value})
	if err != nil {
		return 0, err
	}
	if "ok" != resp[0].String() {
		return 0, errors.New(resp[0].String())
	}
	return Int64(resp)
}
func (db *SSDB) QPopFront(name string) (string, error) {

	resp, err := db.conn.Do("qpop_front", []interface{}{name})
	if err != nil {
		return "", err
	}
	if "ok" != resp[0].String() {
		return "", errors.New(resp[0].String())
	}
	return StringValue(resp)
}
func (db *SSDB) QPopBack(name string) (string, error) {
	resp, err := db.conn.Do("qpop_back", []interface{}{name})
	if err != nil {
		return "", err
	}
	if "ok" != resp[0].String() {
		return "", errors.New(resp[0].String())
	}
	return StringValue(resp)
}
func (db *SSDB) QSize(name string) (int64, error) {
	resp, err := db.conn.Do("qsize", []interface{}{name})
	if err != nil {
		return 0, err
	}
	if "ok" != resp[0].String() {
		return 0, errors.New(resp[0].String())
	}
	return Int64(resp)
}
func (db *SSDB) QList(name_start, name_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("qlist", []interface{}{name_start, name_end, limit})
	if err != nil {
		return nil, err
	}
	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringArray(resp)
}
func (db *SSDB) QRlist(name_start, name_end string, limit int) ([]string, error) {
	resp, err := db.conn.Do("qrlist", []interface{}{name_start, name_end, limit})
	if err != nil {
		return nil, err
	}
	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringArray(resp)
}
func (db *SSDB) QClear(name string) (bool, error) {
	resp, err := db.conn.Do("qclear", []interface{}{name})
	if err != nil {
		return false, nil
	}
	if "ok" != resp[0].String() {
		return false, errors.New(resp[0].String())
	}

	return "ok" == resp[0].String(), nil
}
func (db *SSDB) QFront(name string) (string, error) {
	resp, err := db.conn.Do("qfront", []interface{}{name})
	if err != nil {
		return "", err
	}
	if "ok" != resp[0].String() {
		return "", errors.New(resp[0].String())
	}
	return StringValue(resp)
}
func (db *SSDB) QBack(name string) (string, error) {
	resp, err := db.conn.Do("qback", []interface{}{name})
	if err != nil {
		return "", err
	}
	if "ok" != resp[0].String() {
		return "", errors.New(resp[0].String())
	}
	return StringValue(resp)
}
func (db *SSDB) QGet(name string, index int64) (string, error) {
	resp, err := db.conn.Do("qget", []interface{}{name, index})
	if err != nil {
		return "", err
	}
	if "ok" != resp[0].String() {
		return "", errors.New(resp[0].String())
	}
	return StringValue(resp)
}
func (db *SSDB) QSlice(name string, begin, end int64) ([]string, error) {
	resp, err := db.conn.Do("qslice", []interface{}{name, begin, end})
	if err != nil {
		return nil, err
	}
	if "ok" != resp[0].String() {
		return nil, errors.New(resp[0].String())
	}
	return StringArray(resp)
}
