package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

/*
Your company has a large distributed system that generates log messages continuously. Log messages are produced by three subsystems - A, B, and C. Each subsystem produces a single line of log independently and concurrently with other subsystems, and each one notifies your log processing system once it has produced its log line. Due to the specifics of the system, it's crucial that logs from subsystems are processed in the order A -> B -> C for each cycle.

*Problem Statement*

*Design and implement a concurrent log processing system that ensures the log lines from the subsystems A, B, and C are always processed in the correct order (A -> B -> C), even though the subsystems generate log lines independently and concurrently.

*Requirements*

Each subsystem is represented by a function in your code: printFirst(), printSecond(), and printThird(). When these functions are called, they will print a log line. These functions may be called concurrently. You must ensure that the log lines are always processed (i.e., printed) in the order: first A, then B, and then C.

*Bonus*

The system should be robust, i.e., a temporary failure of one subsystem should not disrupt the whole process. In case a subsystem fails, the system should be able to resume processing once the subsystem is back online.

*Evaluation*

Your solution would be evaluated based on the following:

- Correctness of synchronization and concurrency mechanisms.
- Efficiency of the operation.
- Robustness of your solution against temporary failures.
- Code organization, readability, and commenting.

*Result*

In this real-world inspired problem, the candidate must ensure that log lines are processed in a specific order, similar to the "Print in Order" problem. The solution should effectively handle concurrency and synchronization to maintain the correct sequence. The bonus challenge adds an element of fault tolerance, which is important in a real-world system.
*/

type SubSystem string

const (
	A SubSystem = "A"
	B SubSystem = "B"
	C SubSystem = "C"
)

type LogEvent struct {
	Name     SubSystem
	Metadata string
}

// Representation of the system. Use to generate printFirst, printSecond, and printThird
func mockSubsystem(name string) LogEvent {
	random := time.Duration(rand.Intn(2000))
	time.Sleep(random * time.Millisecond)
	// add failure test 10% of the time
	if random >= 1800 {
		panic("SYSTEM PANIC")
	}
	fmt.Printf("a log from system %s\n", name)

	return LogEvent{Name: SubSystem(strings.ToUpper(name)), Metadata: fmt.Sprintf("%s: [log metada]", strings.ToUpper(name))}
}

// Maps the system even in the channel for future processing
func connectToSubsystem(systemName string, done <-chan interface{}, errChan chan<- SubSystem) <-chan LogEvent {
	out := make(chan LogEvent)
	go func() {
		defer close(out)
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("%%%%%%%%%%%%\nERROR: system %s had an error:\n%v\n%%%%%%%%%%%%%%%%\n", systemName, r)
				errChan <- SubSystem(systemName)
			}
		}()
		for {
			select {
			case <-done:
				return
			case out <- mockSubsystem(systemName):
			}
		}
	}()
	return out
}

// Pipe all the subsystems channels into one so that the window channel only cares about one channel
func mergeStreams(done <-chan interface{}, errChan chan SubSystem, channels []<-chan LogEvent) <-chan LogEvent {
	mergedStream := make(chan LogEvent, len(channels))
	var wg sync.WaitGroup

	wg.Add(len(channels)) // we want to make sure we wait for all channels to be drained

	for _, ch := range channels {
		go func(c <-chan LogEvent) {
			defer wg.Done()
			for log := range c {
				select {
				case <-done:
					return
				case mergedStream <- log:
				}
			}
		}(ch)
	}

	// In Case of error we want to re-create the connection with the subsystem with a new channel
	go func() {
		for {
			var errSystemName SubSystem
			select {
			case <-done:
				return
			case errSystemName = <-errChan:
				wg.Add(1)
				newChannel := connectToSubsystem(string(errSystemName), done, errChan)
				go func(c <-chan LogEvent) {
					defer wg.Done()
					for log := range c {
						select {
						case <-done:
							return
						case mergedStream <- log:
						}
					}
				}(newChannel)

			}
		}
	}()

	// we wait for all channels to drain before we close mergeStreams
	go func() {
		wg.Wait()
		fmt.Println("all subsystems are drained")
		close(mergedStream)
	}()

	return mergedStream
}

/*
Since we want to process data in order A->B->C for each Cycle (this case 1 second)
we need a window channel to aggregate before processing
*/
func window(done <-chan interface{}, in <-chan LogEvent) <-chan LogEvent {
	from := time.Now()
	window := make(chan LogEvent)
	go func() {
		for {
			select {
			case <-done:
				return
			case window <- <-in:
			}

			if time.Since(from) >= time.Second {
				close(window)
				return
			}
		}
	}()

	return window
}

func main() {
	done := make(chan interface{})
	errChan := make(chan SubSystem)
	defer close(done)
	defer close(errChan)
	channels := []<-chan LogEvent{connectToSubsystem("a", done, errChan), connectToSubsystem("b", done, errChan), connectToSubsystem("c", done, errChan)}

	mainChannel := mergeStreams(done, errChan, channels)

	for {
		queue := make(map[SubSystem][]string)
		for w := range window(done, mainChannel) {
			if queue[w.Name] == nil {
				queue[w.Name] = make([]string, 0)
			}
			queue[w.Name] = append(queue[w.Name], w.Metadata)
		}
		fmt.Println("Processing:")
		fmt.Printf("A = %v \n B = %v \n C = %v \n", queue[SubSystem("A")], queue[SubSystem("B")], queue[SubSystem("C")])
		fmt.Println("Processing completed")
	}
}
