package mq_test

import (
	"fmt"
	"log"

	"github.com/go-mq/mq/v2"
	_ "github.com/go-mq/mq/v2/memory"
)

func ExampleMemoryQueue() {
	b, err := mq.NewBroker("memory://")
	if err != nil {
		log.Fatal(err)
	}

	q, err := b.Queue("test-queue")
	if err != nil {
		log.Fatal(err)
	}

	j := mq.NewJob()

	if err := j.Encode("hello world!"); err != nil {
		log.Fatal(err)
	}

	if err := q.Publish(j); err != nil {
		log.Fatal(err)
	}

	iter, err := q.Consume(1)

	consumedJob, err := iter.Next()
	if err != nil {
		log.Fatal(err)
	}

	var payload string
	if err := consumedJob.Decode(&payload); err != nil {
		log.Fatal(err)
	}

	fmt.Println(payload)
	// Output: hello world!
}
