package main

import (
	"sort"
	"strconv"
	"sync"
	"runtime"
)

type job func(in, out chan interface{})

var SingleHash = func (in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	var SingleHashing = func (data interface{}, dataString string, dataStringMd5 string, out chan interface{}) {
		defer wg.Done()
		wg2 := &sync.WaitGroup{}
		var item1 string
		var item2 string
		var wrapperFunc = func(myFunc func(), wg2 *sync.WaitGroup) {
			defer wg2.Done()
			myFunc()
		}
		var crcFunc = func() {
			item1 = DataSignerCrc32(dataString) + "~"
		}
		var md5Func = func() {
			item2 = DataSignerCrc32(dataStringMd5)
		}
		funcList := []func(){crcFunc, md5Func}
		for _, funcName := range funcList {
			wg2.Add(1)
			go wrapperFunc(funcName, wg2)
			runtime.Gosched()
		}
		wg2.Wait()
		out <- item1 + item2
	}
	for data := range in {
		wg.Add(1)
		go SingleHashing(data, strconv.Itoa(data.(int)), DataSignerMd5(strconv.Itoa(data.(int))), out)
	}
	wg.Wait()
}

var MultiHash = func (in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	var MultiHashing = func (data interface {}, out chan interface{}) {
		const (
			iterationNum = 6
		)
		var hashSlice [iterationNum]string
		defer wg.Done()
		wg2 := &sync.WaitGroup{}
		stringData := data.(string)
		var wrapperFunc = func(myFunc func(iterNum int), iterNum int) {
			defer wg2.Done()
			myFunc(iterNum)
		}
		var MultiSignerCrd = func(iterNum int) {
			hashSlice[iterNum] = DataSignerCrc32(strconv.Itoa(iterNum) + stringData)
		}
		for i := 0; i < iterationNum; i++ {
			wg2.Add(1)
			go wrapperFunc(MultiSignerCrd, i)
		}
		wg2.Wait()
		var tempString string
		for _, dataTemp := range hashSlice {
			tempString += dataTemp
		}
		out <- tempString
	}
	for data := range in {
		wg.Add(1)
		go MultiHashing(data, out)
	}
	wg.Wait()
}

var CombineResults = func (in, out chan interface{}) {
	var combineHashedValue string
	var hashSlice []string
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var CombineCalculating = func (data interface{}) {
		defer wg.Done()
		mu.Lock()
		hashSlice = append(hashSlice, (data.(string) + "_"))
		mu.Unlock()
	}
	for data := range in {
		wg.Add(1)
		go CombineCalculating(data)
	}
	wg.Wait()
	mu.Lock()
	sort.Strings(hashSlice)
	for i := 0; i <= len(hashSlice) - 1; i++ {
		combineHashedValue += hashSlice[i]
	}
	out <- combineHashedValue[:len(combineHashedValue)-1]
	mu.Unlock()
}

func ExecutePipeline(process ...job) {
	var channelsList []chan interface{}
	wg := &sync.WaitGroup{}
	var wrapperFunc = func(myFunc job, in, out chan interface{}) {
		defer wg.Done()
		myFunc(in, out)
		close(out)
	}
	for i:= 0; i <= len(process) + 1; i++ {
		channelsList = append(channelsList, make(chan interface{}, 1))
	}
	for i, function := range process {
		wg.Add(1)
		go wrapperFunc(function, channelsList[i], channelsList[i+1])
	}
	wg.Wait()
}