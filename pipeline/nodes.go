package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"
	"math/rand"
)

var startTime time.Time

func Init() {
	startTime = time.Now()
}

// 数据源来自于一个Array
func ArraySource(a ...int) <-chan int {
	// 调用的真实情况是，函数新建一个channel并马上返回，并行的goroutine来进行发送数据的操作，发送完后记得close。
	// func: 1.新建一个channel
	out := make(chan int)
	// go: 1.发送数据（channel是goroutine之间的通信管道）
	go func() {
		for _, v := range a {
			out <- v
		}
		// go: 2.关闭channel，否则会报错：fatal error: all goroutines are asleep - deadlock!
		close(out)
	}()
	// func: 2.返回这个channel
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		// Read into memory
		a := []int{}
		for v := range in {
			a = append(a, v)
		}
		fmt.Println("Read done:", time.Now().Sub(startTime))

		// Sort
		sort.Ints(a)
		fmt.Println("InMemSort done:", time.Now().Sub(startTime))

		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

// 归并排序
func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		close(out)
		fmt.Println("Merge done:", time.Now().Sub(startTime))
	}()
	return out
}

// 随机生成count个int型数据
func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

// 读数据。第一个参数是读的来源对象，第二个参数是读取长度(-1全读)。输出是一个channel
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		// 64位系统的int型大小是64，所以用一个64位buffer = byte(8)*8
		buffer := make([]byte, 8)
		// 读取长度的控制变量
		bytesRead := 0
		for {
			// n是读取的长度
			n, err := reader.Read(buffer)
			bytesRead += n
			// 可能最后读取4字节数据，nil=EOF，所以要先读取数据，再判断nil
			if n > 0 {
				// 大端还是小端，发送和接收端统一即可
				out <- int(binary.BigEndian.Uint64(buffer))
			}
			if err != nil ||
				(chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

// 写数据。第一个参数是写的目的对象，第二个参数是写的数据channel
func WriteSink(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}
}

// 搭建归并节点组，递归调用实现2路归并
func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	m := len(inputs) / 2
	// merge inputs[0..m) and inputs [m..end)
	return Merge(
		MergeN(inputs[:m]...),
		MergeN(inputs[m:]...))
}
