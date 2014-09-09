ssdb
====

ssdb client for go

sample
====
```go
package main
import (
	"fmt"
	"time"
	"github.com/jiecao-fm/ssdb"
)

var (
	conn_timeout  time.Duration = 15 * time.Second
	read_timeout  time.Duration = 180 * time.Second
	write_timeout time.Duration = 0
)

func main(){
  db, err := ssdb.Connect("jiecao-tucao", 8888, conn_timeout, read_timeout, write_timeout)
	if err != nil {
		fmt.Printf("connect to server failed:\n%v", err)
		return
	}
	db.Set("key1", "value1")
	ex, _ := db.Exists("key1")
	value,_:=db.Get("key1")
	fmt.Printf("%s\n",value)
  db.Close()

}
```
pool sample
===
```go
package main
import (
	"fmt"
	"sync"
	"testing"
)
func main(){
count := 3
	g := sync.WaitGroup{}
	g.Add(count)
	poolconf := PoolConfig{Host: "jiecao-tucao", Port: 8888, Initial_conn_count: 1, Max_idle_count: 3, Max_conn_count: 8}
	pool, err := NewPool(poolconf)
	if err != nil {
		return
	}
	defer pool.Close()

	for i := 0; i < count; i++ {
		k := i
		go func() {
			db, _ := pool.GetDB()

			name := "branch_" + fmt.Sprintf("%d", k)
			defer func() {
				pool.ReturnDB(db)
				fmt.Printf("idle count:%d\n", pool.IdleCount())
				g.Done()
			}()

			for j := 0; j < 20; j++ {
				key := name + "lxy" + fmt.Sprintf("%d", j)
				db.Set(key, "value"+fmt.Sprintf("%d", j))
				value, _ := db.Get(key)
				fmt.Printf("%s %s\n", name, value)
				db.Del(key)
			}

		}()
	}
	g.Wait()
}
```
