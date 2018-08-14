package main

import (
	"testing"
	"externalsort/pipeline"
	"fmt"
	)

func TestArraySource(t *testing.T) {
	// 1. channel通信
	p := pipeline.ArraySource(3, 2, 6, 7, 4)
	// (1)
	//for {
	//	if num, ok := <-p; ok {
	//		fmt.Println(num)
	//	} else {
	//		break
	//	}
	//}

	//(2) 简略写法
	for v := range p {
		fmt.Println(v)
	}
}

func TestInMemSort(t *testing.T) {
	// 初始化时钟打印日志
	pipeline.Init()

	// 2. 内部排序
	p := pipeline.InMemSort(
		pipeline.ArraySource(3, 2, 6, 7, 4))
	for v := range p {
		fmt.Println(v)
	}
}

func TestMerge(t *testing.T) {
	pipeline.Init()

	// 3. 归并排序
	p := pipeline.Merge(
		pipeline.InMemSort(pipeline.ArraySource(3, 2, 6, 7, 4)),
		pipeline.InMemSort(pipeline.ArraySource(7, 4, 0, 3, 2, 8, 13)))
	for v := range p {
		fmt.Println(v)
	}
}

func TestRandomSource(t *testing.T) {
	// 生成随机数
	p := pipeline.RandomSource(10)
	for v := range p {
		fmt.Println(v)
	}
}

