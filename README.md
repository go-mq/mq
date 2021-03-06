# mq [![GoDoc](https://godoc.org/gopkg.in/go-mq/mq.v1?status.svg)](https://godoc.org/github.com/go-mq/mq) ![Go](https://github.com/go-mq/mq/workflows/Go/badge.svg?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/go-mq/mq)](https://goreportcard.com/report/github.com/go-mq/mq)

MQ is a generic interface to abstract the details of implementation of messaging queues
systems.

Similar to the package [`database/sql`](https://golang.org/pkg/database/sql/),
this package implements a common interface to interact with different message queue
systems, in a unified way.

Installation
------------

The recommended way to install *mq* is:

```
go get github.com/go-mq/mq/v2
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
    "github.com/go-mq/mq/v2"
    _ "github.com/go-mq/mq/v2/memory"
    "log"
)

//...
func main() {
    b, _ := mq.NewBroker("memory://")
    q, _ := b.Queue("test-queue")
    j := mq.NewJob()
    
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

License
-------
Apache License Version 2.0, see [LICENSE](LICENSE)
