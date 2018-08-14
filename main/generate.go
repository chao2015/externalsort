package main

import (
	"externalsort/pipeline"
	"os"
	"bufio"
	"fmt"
)

// STEP 1
// 生成 small.in 或 large.in
func main() {
	// (1) small.in 64
	// (2) large.in 10000000
	const filename = "small.in"
	const n = 64

	// 新建文件，返回可用的文件描述符
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 生成随机数
	p := pipeline.RandomSource(n)

	// 写数据到文件
	// 包装文件描述符，使用缓存机制，提高读写速度
	writer := bufio.NewWriter(file)
	pipeline.WriteSink(writer, p)
	writer.Flush() // 确保缓存数据全部写入

	// 上面的文件描述符offset在末尾，不能用于读取
	// 打开文件
	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 读取数据
	p = pipeline.ReaderSource(bufio.NewReader(file), -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}
