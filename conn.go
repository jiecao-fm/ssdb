package ssdb

//Conn impl
import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	//"encoding/binary"
)

type conn struct {
	connected bool
	client    net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	err       error
	recv_buf  bytes.Buffer
}

func (c *conn) Close() error {
	c.client.Close()
	c.connected = false
	return nil
}

func (c *conn) Err() error {
	return c.err
}

func (c *conn) Do(cmd string, args []interface{}) (rsp []bytes.Buffer, err error) {
	err = c.Send(cmd, args[:])
	if err != nil {
		return nil, err
	}
	c.Flush()
	return c.Receive()
}

func (c *conn) Send(cmd string, args []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%d", len(cmd)))
	buf.WriteByte('\n')
	buf.WriteString(cmd)
	buf.WriteByte('\n')
	for _, arg := range args {
		var block []byte
		switch arg := arg.(type) {
		case int:
			{
				s := strconv.Itoa(arg)
				block = []byte(s)
				writeBlock(&buf, block)
			}
		case int64:
			{
				s := strconv.FormatInt(arg, 10)
				block = []byte(s)
				writeBlock(&buf, block)
			}
		case string:
			{
				block = []byte(arg)
				writeBlock(&buf, block)
			}

		case byte:
			{
				block = []byte{arg}
				writeBlock(&buf, block)
			}

		case []byte:
			{
				writeBlock(&buf, arg)
			}

		case []string:
			{
				for _, a := range arg {
					buf.WriteString(fmt.Sprintf("%d", len(a)))
					buf.WriteByte('\n')
					buf.WriteString(a)
					buf.WriteByte('\n')
				}

			}
		case float64:
			{

			}
		case bool:
			{

			}
		}
	}
	buf.WriteByte('\n')
	//	fmt.Printf(buf.String() + "\n")
	_, err := c.writer.Write(buf.Bytes())
	if err != nil {
		fmt.Printf("error:%v", err)
	}
	return err
}

func writeBlock(buf *bytes.Buffer, bs []byte) {
	buf.WriteString(fmt.Sprintf("%d", len(bs)))
	buf.WriteByte('\n')
	buf.Write(bs)
	buf.WriteByte('\n')
}

//flushes the output buffer to the server
func (c *conn) Flush() error {
	return c.writer.Flush()
}

//receives a single reply from server
func (c *conn) Receive() (res []bytes.Buffer, err error) {
	var bufArray = []bytes.Buffer{} //make([]bytes.Buffer,5)

	for {
		var sizebuf bytes.Buffer
		//read size
		for b, er := c.reader.ReadByte(); b != '\n'; b, er = c.reader.ReadByte() {
			if er != nil {
				fmt.Printf("%v\n", er)
				return nil, er
			}
			if b != '\r' {
				sizebuf.WriteByte(b)
			}
		}
		//end of packet
		if sizebuf.Len() == 0 {
			return bufArray[0:], nil
		}

		size, er := strconv.Atoi(sizebuf.String())
		if er != nil {
			return nil, er
		}
		dataBytes := make([]byte, size)
		count, er := c.reader.Read(dataBytes)
		if count != size {
			fmt.Println("read count != count")
			return nil, nil
		}
		var dataBuf bytes.Buffer
		dataBuf.Write(dataBytes)
		bufArray = append(bufArray, dataBuf)
		//		fmt.Println(string(dataBuf))
		for b, er := c.reader.ReadByte(); b != '\n'; b, er = c.reader.ReadByte() {
			if er != nil {
				return nil, er
			}
		}
	}

	//never execute here
	fmt.Printf("buf size:%d\n", len(bufArray))
	return bufArray[0:], nil
}
