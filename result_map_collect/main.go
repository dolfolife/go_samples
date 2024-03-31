package main

import (
	"fmt"
	"io"
	"net/http"
)

func main() {

	urls := []string{"https://google.com", "https://testurl.com", "notAHost:30303030"}

	responses := Map(NewSliceResult(urls), resultWrapper(fetchUrl))
	bodies := Map(responses, resultWrapper(extractBody))
	readBodies := Map(bodies, resultWrapper(parseBody))
	pagesLen := Map(readBodies, resultWrapper(pageLen))
	arePagesLong := Map(pagesLen, resultWrapper(isPageLong))

	for _, v := range Collect(arePagesLong) {
		fmt.Println(v)
	}
}

func resultWrapper[T any, V any](f func(T) (V, error)) func(Result[T]) Result[V] {
	return func(r Result[T]) Result[V] {
		if r.Error != nil {
			return Result[V]{Error: r.Error}
		}

		v, err := f(r.Val)
		return Result[V]{Val: v, Error: err}
	}
}

func fetchUrl(url string) (*http.Response, error) {
	return http.Get(url)
}

func extractBody(resp *http.Response) (io.ReadCloser, error) {
	return resp.Body, nil
}

func parseBody(body io.ReadCloser) ([]byte, error) {
	defer body.Close()

	return io.ReadAll(body)
}

func pageLen(pageBody []byte) (int, error) {
	return len(pageBody), nil
}

func isPageLong(l int) (bool, error) {
	return l > 100, nil
}

type Iterator[T any] interface {
	Next() bool
	Value() T
}

type Result[V any] struct {
	Val   V
	Error error
}

type SliceResult[T any] struct {
	Results []Result[T]
	value   Result[T]
	index   int
}

func (s *SliceResult[T]) Next() bool {

	if s.index < len(s.Results) {
		s.value = s.Results[s.index]
		s.index += 1
		return true
	}

	return false
}

func (s *SliceResult[T]) Value() Result[T] {
	return s.value
}

func NewSliceResult[T any](elems []T) Iterator[Result[T]] {
	result := SliceResult[T]{}
	for _, e := range elems {
		result.Results = append(result.Results, Result[T]{Val: e, Error: nil})
	}
	return &result
}

type mapIterator[T any, V any] struct {
	source Iterator[T]
	mapper func(T) V
}

func (iter *mapIterator[T, V]) Next() bool {
	return iter.source.Next()
}

func (iter *mapIterator[T, V]) Value() V {
	value := iter.source.Value()
	return iter.mapper(value)
}

func Map[T any, V any](iter Iterator[T], f func(T) V) Iterator[V] {
	return &mapIterator[T, V]{
		iter, f,
	}
}

func Collect[T any](iter Iterator[T]) []T {
	var xs []T

	for iter.Next() {
		xs = append(xs, iter.Value())
	}

	return xs
}
