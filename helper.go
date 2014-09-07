package ssdb

import (
	"bytes"
	"fmt"
	"strconv"
)

func BoolValue(rsp []bytes.Buffer) (bool, error) {
	if rsp[0].String() != "ok" {
		return false, fmt.Errorf(rsp[0].String())
	}

	return rsp[1].String() == "1", nil
}
func Int64(rsp []bytes.Buffer) (int64, error) {
	if rsp[0].String() != "ok" {
		return 0, fmt.Errorf(rsp[0].String())
	}
	res, er := strconv.ParseInt(rsp[1].String(), 10, 64)
	return res, er
}

func IntValue(rsp []bytes.Buffer) (int, error) {
	if rsp[0].String() != "ok" {
		return 0, fmt.Errorf(rsp[0].String())
	}
	return strconv.Atoi(rsp[1].String())

}

func StringValue(rsp []bytes.Buffer) (string, error) {
	if rsp[0].String() != "ok" {
		return "", fmt.Errorf(rsp[0].String())
	}

	return rsp[1].String(), nil
}
func StringArray(rsp []bytes.Buffer) ([]string, error) {
	if rsp[0].String() != "ok" {
		return nil, fmt.Errorf(rsp[0].String())
	}
	var res []string
	for _, buf := range rsp[1:] {
		res = append(res, buf.String())
	}

	return res, nil
}

func Int64Map(rsp []bytes.Buffer) (map[string]int64, error) {
	if rsp[0].String() != "ok" {
		return nil, fmt.Errorf(rsp[0].String())
	}
	m := make(map[string]int64)
	key := ""
	for _, buf := range rsp[1:] {
		if key == "" {
			key = buf.String()
		} else {
			m[key], _ = strconv.ParseInt(buf.String(), 10, 64)
			key = ""
		}
	}
	return m, nil
}

func StringMap(rsp []bytes.Buffer) (map[string]string, error) {

	if rsp[0].String() != "ok" {
		return nil, fmt.Errorf(rsp[0].String())
	}
	m := make(map[string]string)
	key := ""
	for _, buf := range rsp[1:] {
		if key == "" {
			key = buf.String()
		} else {
			m[key] = buf.String()
			key = ""
		}
	}
	return m, nil
}
