package main

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type UrlData struct {
	Url  string
	Body string
}

type Result struct {
	Data  UrlData
	Error error
}

/* download is the sync code we cannot break down nor run its parts in parallel */
func download(url string) (UrlData, error) {

	r, errorGet := http.Get(url)

	if errorGet != nil {
		return UrlData{Url: url}, errorGet
	}

	defer r.Body.Close()

	body, errorRead := io.ReadAll(r.Body)

	return UrlData{Body: string(body), Url: url}, errorRead
}

/*
* Question:
* Implement a function that, given an array of URLs and an existing download function,
* downloads all the data from the urls in parallel, merges the results into a single
* dictionary of {url:data} and then returns the dictionary.
*
 */
func main() {

	urls := []string{
		"https://testurl.com/",
		"https://testurl.com/",
		"https://localhost:3000/",
		"https://testurl.com/",
		"https://testurl.com/",
		"https://testurl.com/",
		"https://testurl.com/",
		"https://testurl.com/",
		"https://testurl.com/",
		"https://testurl.com/",
		"https://google.com/",
	}

	start := time.Now()

	done := make(chan interface{})
	defer close(done)
	for res := range fetchAndMergeUrlData(urls, done) {
		if res.Error != nil {
			fmt.Println(res.Error)
			continue
		}
		fmt.Printf("URL %s has a body of size %d\n", res.Data.Url, len(res.Data.Body))
	}
	fmt.Printf("took %v\n", time.Since(start))
}

func fetchAndMergeUrlData(urls []string, done <-chan interface{}) <-chan Result {
	result := make(chan Result, len(urls))

	go func() {
		defer close(result)

		for _, url := range urls {
			data, error := download(url)
			select {
			case <-done:
				return
			case result <- Result{Data: data, Error: error}:
			}
		}
	}()

	return result
}
