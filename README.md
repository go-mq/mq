# mq [![GoDoc](https://godoc.org/gopkg.in/go-mq/mq.v1?status.svg)](https://godoc.org/github.com/go-mq/mq) [![Go Report Card](https://goreportcard.com/badge/github.com/go-mq/mq)](https://goreportcard.com/report/github.com/go-mq/mq)

MQ is a generic interface to abstract the details of implementation of messaging queues
systems.

Similar to the package [`database/sql`](https://golang.org/pkg/database/sql/),
this package implements a common interface to interact with different message queue
systems, in a unified way.

Currently, only AMQP queues and an in-memory queue are supported.

Installation
------------

The recommended way to install *mq* is:

```
go get gopkg.in/mq.v1
```

Usage
-----

This example shows how to publish and consume a Job from the in-memory
implementation, very useful for unit tests.

The mq implementations to be supported by the `NewBroker` should be imported
as shows the example.

```go
package main

import (
    "fmt"
	"gopkg.in/mq.v1"
	_ "gopkg.in/mq.v1/memory"
    "log"
)

//...
func main() {
    b, _ := queue.NewBroker("memory://")
    q, _ := b.Queue("test-queue")
    j := queue.NewJob()
    
    if err := q.Publish(j); err != nil {
        log.Fatal(err)
    }
    
    iter, err := q.Consume(1)
    if err != nil {
        log.Fatal(err)
    }
    
    consumedJob, _ := iter.Next()
    
    var payload string
    _ = consumedJob.Decode(&payload)
    
    fmt.Println(payload)
    // Output: hello world!
}
```


Configuration
-------------

### AMQP

The list of available variables is:

- `AMQP_BACKOFF_MIN` (default: 20ms): Minimum time to wait for retry the connection or queue channel assignment.
- `AMQP_BACKOFF_MAX` (default: 30s): Maximum time to wait for retry the connection or queue channel assignment.
- `AMQP_BACKOFF_FACTOR` (default: 2): Multiplying factor for each increment step on the retry.
- `AMQP_BURIED_QUEUE_SUFFIX` (default: `.buriedQueue`): Suffix for the buried queue name.
- `AMQP_BURIED_EXCHANGE_SUFFIX` (default: `.buriedExchange`): Suffix for the exchange name.
- `AMQP_BURIED_TIMEOUT` (default: 500): Time in milliseconds to wait for new jobs from the buried queue.
- `AMQP_RETRIES_HEADER` (default: `x-retries`): Message header to set the number of retries.
- `AMQP_ERROR_HEADER` (default: `x-error-type`): Message header to set the error type.

License
-------
Apache License Version 2.0, see [LICENSE](LICENSE)