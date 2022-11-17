package mr

import (
	"fmt"
	"hash/fnv"
	"testing"
)

func Test_ihash(t *testing.T) {
	a := ihash1("A") % 10
	fmt.Println(a)

}

func ihash1(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
