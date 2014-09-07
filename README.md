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
