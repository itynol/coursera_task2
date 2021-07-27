package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})

	wg := &sync.WaitGroup{}
	for _, singleJob := range jobs {
		wg.Add(1)
		out := make(chan interface{})
		go func(singleJob job, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			singleJob(in, out)
		}(singleJob, in, out)
		in = out
	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	for data := range in {
		stringValue := stringConverter(data)
		md5Hash := make(chan string)
		wg.Add(1)
		go func(data string, md5Hash chan<- string) {
			defer wg.Done()
			mutex.Lock()
			md5Data := DataSignerMd5(stringValue)
			mutex.Unlock()
			md5Hash <- DataSignerCrc32(md5Data)
		}(stringValue, md5Hash)
		wg.Add(1)
		go func(data string, out chan<- interface{}, md5Hash <-chan string) {
			defer wg.Done()
			crc32 := DataSignerCrc32(data)
			md5 := <-md5Hash
			out <- crc32 + "~" + md5
		}(stringValue, out, md5Hash)
	}
	wg.Wait()
}

func stringConverter(data interface{}) string {
	switch data.(type) {
	case string:
		return data.(string)
	case int:
		return strconv.Itoa(data.(int))
	default:
		return ""
	}
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	for data := range in {
		stringValue := stringConverter(data)
		resultStrings := make([]string, 6)
		internalWG := &sync.WaitGroup{}
		for i := 0; i < 6; i++ {
			internalWG.Add(1)
			go func(stringValue string, i int) {
				defer internalWG.Done()
				str := DataSignerCrc32(strconv.Itoa(i) + stringValue)
				mutex.Lock()
				resultStrings[i] = str
				mutex.Unlock()
			}(stringValue, i)
		}
		wg.Add(1)
		go func(out chan<- interface{}) {
			defer wg.Done()
			internalWG.Wait()
			res := strings.Join(resultStrings, "")
			out <- res
		}(out)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	resSlice := make([]string, 0)
	for data := range in {
		resSlice = append(resSlice, data.(string))
	}
	sort.Slice(resSlice, func(i, j int) bool {
		return resSlice[i] < resSlice[j]
	})
	res := strings.Join(resSlice, "_")
	out <- res
}
