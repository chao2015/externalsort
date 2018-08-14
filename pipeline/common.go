package pipeline

import (
	"os"
	"learngo/pipeline"
	"fmt"
	"bufio"
	"strconv"
)

func PrintFile(filename string) {
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 读取数据
	p := pipeline.ReaderSource(file, -1)
	count := 0
	fmt.Println("Sorted:")
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func WriteToFile(p <-chan int, filename string) {
	// 创建文件
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 写入文件
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	pipeline.WriteSink(writer, p)
}

// pipeline的搭建及运行，单机上时，分块数(chunkCount)最好是cpu的核数
func CreatePipeline(
	filename string,
	fileSize, chunkCount int) <-chan int {

	chunkSize := fileSize / chunkCount
	pipeline.Init()

	// 初始化结点组
	sortResults := []<-chan int{}
	for i := 0; i < chunkCount; i++ {
		// 打开文件
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		// 设置offset
		file.Seek(int64(i*chunkSize), 0)
		// 读取数据
		source := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)
		// 内部排序后，追加到结点组中
		sortResults = append(sortResults, pipeline.InMemSort(source))
	}

	// 归并结点组
	return pipeline.MergeN(sortResults...)
}

func CreateNetworkPipeline(
	filename string,
	fileSize, chunkCount int) <-chan int {

	chunkSize := fileSize / chunkCount
	pipeline.Init()

	// 初始化端口号组
	sortAddr := []string{}
	for i := 0; i < chunkCount; i++ {
		// 打开文件
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		// 设置offset
		file.Seek(int64(i*chunkSize), 0)
		// 读取数据
		source := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)
		// 端口号
		addr := ":" + strconv.Itoa(7000+i)
		// 起tcp服务
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		// 追加到端口号组
		sortAddr = append(sortAddr, addr)
	}

	// 初始化结点组
	sortResults := []<-chan int{}
	for _, addr := range sortAddr {
		sortResults = append(sortResults, pipeline.NetworkSource(addr))
	}

	// 归并结点组
	return pipeline.MergeN(sortResults...)
}