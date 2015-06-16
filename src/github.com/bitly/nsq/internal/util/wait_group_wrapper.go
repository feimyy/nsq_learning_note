package util

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	//	fmt.Printf("WaitGroupWrapper.Wrap() : Starting WaitGroup.Add(1)\n")
	_, file1, line1, _ := runtime.Caller(1)
	_, file2, line2, _ := runtime.Caller(2)
	_, file3, line3, _ := runtime.Caller(3)
	file1 = file1[strings.LastIndex(file1, "/")+1:] //只获取文件名
	file2 = file2[strings.LastIndex(file2, "/")+1:] //只获取文件名
	file3 = file3[strings.LastIndex(file3, "/")+1:] //只获取文件名

	fmt.Printf("WaitGroupWrapper.Wrap() : New Calling :\n\ttop_caller :%s:%d\n\tmedium_caller :%s:%d\n\tlast_caller : %s:%d\n",
		file1, line1,
		file2, line2,
		file3, line3)
	w.Add(1)
	go func() {
		cb()
		fmt.Printf("WaitGroupWrapper.Wrap() : Finish Execute Function\n")
		w.Done()
	}()
	//	fmt.Printf("WaitGroupWrapper.Wrap() :Finish WaitGroup\n")
}
