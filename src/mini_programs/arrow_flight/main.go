package main

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

func main() {
	nums := []int32{10, 20, 30, 40, 50, 60, 70, 80}

	// make buffer
	buf := make([]byte, 4*len(nums)) // int32 takes 4 byte

	// put numbers in to the buffer
	for i, n := range nums {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(n))
	}

	// Zero-copy: take a pointer to the start of []byte and treat it as a []int32 slice
	intSlice := unsafe.Slice((*int32)(unsafe.Pointer(&buf[0])), len(buf)/4)

	fmt.Println(intSlice)

	intSlice[0] = 999
	fmt.Println("Buffer after change:", buf)

	// For strings, we also need to provide an offset buffer that indicates
	// the starting and ending positions of each string in the data buffer.
}
