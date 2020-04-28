package test

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/src-d/go-queue.v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var testRand *rand.Rand

func init() {
	testRand = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func NewName() string {
	return fmt.Sprintf("queue_tests_%d", testRand.Int())
}

type QueueSuite struct {
	suite.Suite
	r rand.Rand

	TxNotSupported bool
	BrokerURI      string

	Broker queue.Broker
}

func (s *QueueSuite) SetupTest() {
	b, err := queue.NewBroker(s.BrokerURI)
	if !s.NoError(err) {
		s.FailNow(err.Error())
	}

	s.Broker = b
}

func (s *QueueSuite) TearDownTest() {
	s.NoError(s.Broker.Close())
}

func (s *QueueSuite) TestConsume_empty() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	assert.NoError(iter.Close())
}

func (s *QueueSuite) TestJobIter_Next_closed() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	done := s.checkNextClosed(iter)
	assert.NoError(iter.Close())
	<-done
}

func (s *QueueSuite) TestJobIter_Next_empty() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	nJobs := 0

	done := make(chan struct{})
	go func() {
		j, err := iter.Next()
		assert.NoError(err)
		assert.NotNil(j)

		nJobs += 1
		done <- struct{}{}
	}()

	time.Sleep(50 * time.Millisecond)

	assert.Equal(0, nJobs)

	j, err := queue.NewJob()
	assert.NoError(err)

	err = j.Encode(1)
	assert.NoError(err)

	err = q.Publish(j)
	assert.NoError(err)

	<-done

	assert.Equal(1, nJobs)
	assert.NoError(iter.Close())
}

func (s *QueueSuite) TestJob_Reject_no_requeue() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	j, err := queue.NewJob()
	assert.NoError(err)

	err = j.Encode(1)
	assert.NoError(err)

	err = q.Publish(j)
	assert.NoError(err)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	j, err = iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	err = j.Reject(false)
	assert.NoError(err)

	done := s.checkNextClosed(iter)
	time.Sleep(50 * time.Millisecond)
	assert.NoError(iter.Close())
	<-done
}

func (s *QueueSuite) TestJob_Reject_requeue() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	j, err := queue.NewJob()
	assert.NoError(err)

	err = j.Encode(1)
	assert.NoError(err)

	err = q.Publish(j)
	assert.NoError(err)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	j, err = iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	err = j.Reject(true)
	assert.NoError(err)

	j, err = iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	assert.NoError(iter.Close())
}

func (s *QueueSuite) TestPublish_nil() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.Publish(nil)
	assert.True(queue.ErrEmptyJob.Is(err))
}

func (s *QueueSuite) TestPublish_empty() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.Publish(&queue.Job{})
	assert.True(queue.ErrEmptyJob.Is(err))
}

func (s *QueueSuite) TestPublishDelayed_nil() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.PublishDelayed(nil, time.Second)
	assert.True(queue.ErrEmptyJob.Is(err))
}

func (s *QueueSuite) TestPublishDelayed_empty() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.PublishDelayed(&queue.Job{}, time.Second)
	assert.True(queue.ErrEmptyJob.Is(err))
}

func (s *QueueSuite) TestPublishAndConsume_immediate_ack() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	var (
		ids        []string
		priorities []queue.Priority
		timestamps []time.Time
	)
	for i := 0; i < 100; i++ {
		j, err := queue.NewJob()
		assert.NoError(err)
		err = j.Encode(i)
		assert.NoError(err)
		err = q.Publish(j)
		assert.NoError(err)
		ids = append(ids, j.ID)
		priorities = append(priorities, j.Priority)
		timestamps = append(timestamps, j.Timestamp)
	}

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	for i := 0; i < 100; i++ {
		j, err := iter.Next()
		assert.NoError(err)
		assert.NoError(j.Ack())

		var payload int
		assert.NoError(j.Decode(&payload))
		assert.Equal(i, payload)

		assert.Equal(ids[i], j.ID)
		assert.Equal(priorities[i], j.Priority)
		assert.Equal(timestamps[i].Unix(), j.Timestamp.Unix())
	}

	done := s.checkNextClosed(iter)
	assert.NoError(iter.Close())
	<-done
}

func (s *QueueSuite) TestConsumersCanShareJobIteratorConcurrently() {
	assert := assert.New(s.T())
	const (
		nConsumers       int = 10
		nJobs            int = nConsumers
		advertisedWindow int = nConsumers
	)
	queue := s.newQueueWithJobs(nJobs)

	// the iter will be shared by all consumers
	iter, err := queue.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iter)

	// attempt to start several consumers concurrently
	// that never Ack or Reject their jobs
	var allStarted sync.WaitGroup
	allStarted.Add(nConsumers)
	for i := 0; i < nConsumers; i++ {
		go func() {
			_, err := iter.Next()
			assert.NoError(err)
			allStarted.Done()
		}()
	}

	// send true to the done channel when all consumers has started
	done := make(chan bool)
	go func() {
		allStarted.Wait()
		done <- true
	}()

	// wait until all consumers have started or fail after a give up period
	giveUp := time.After(1 * time.Second)
	select {
	case <-done:
		// nop, all consumers started concurrently just fine.
	case <-giveUp:
		assert.FailNow("Give up waiting for consumers to start")
	}
}

// newQueueWithJobs creates and return a new queue with n jobs in it.
func (s *QueueSuite) newQueueWithJobs(n int) queue.Queue {
	assert := assert.New(s.T())

	q, err := s.Broker.Queue(NewName())
	assert.NoError(err)

	for i := 0; i < n; i++ {
		job, err := queue.NewJob()
		assert.NoError(err)
		err = job.Encode(i)
		assert.NoError(err)
		err = q.Publish(job)
		assert.NoError(err)
	}

	return q
}

func (s *QueueSuite) TestDelayed() {
	assert := assert.New(s.T())

	delay := 1 * time.Second

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	j, err := queue.NewJob()
	assert.NoError(err)
	err = j.Encode("hello")
	assert.NoError(err)

	start := time.Now()
	err = q.PublishDelayed(j, delay)
	assert.NoError(err)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)

	var since time.Duration
	for {
		j, err := iter.Next()
		assert.NoError(err)
		if j == nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		since = time.Since(start)

		var payload string
		assert.NoError(j.Decode(&payload))
		assert.Equal("hello", payload)
		break
	}

	assert.True(since >= delay)
}

func (s *QueueSuite) TestTransaction_Error() {
	if s.TxNotSupported {
		s.T().Skip("transactions not supported")
	}

	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.Transaction(func(qu queue.Queue) error {
		job, err := queue.NewJob()
		assert.NoError(err)
		assert.NoError(job.Encode("goodbye"))
		assert.NoError(qu.Publish(job))
		return errors.New("foo")
	})
	assert.Error(err)

	advertisedWindow := 1
	i, err := q.Consume(advertisedWindow)
	assert.NoError(err)

	done := s.checkNextClosed(i)
	time.Sleep(50 * time.Millisecond)
	assert.NoError(i.Close())
	<-done
}

func (s *QueueSuite) TestTransaction() {
	if s.TxNotSupported {
		s.T().Skip("transactions not supported")
	}

	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.Transaction(func(q queue.Queue) error {
		job, err := queue.NewJob()
		assert.NoError(err)
		assert.NoError(job.Encode("hello"))
		assert.NoError(q.Publish(job))
		return nil
	})
	assert.NoError(err)

	advertisedWindow := 1
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)
	var payload string
	assert.NoError(j.Decode(&payload))
	assert.Equal("hello", payload)
	assert.NoError(iter.Close())
}

func (s *QueueSuite) TestTransaction_not_supported() {
	assert := assert.New(s.T())

	if !s.TxNotSupported {
		s.T().Skip("transactions supported")
	}

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	err = q.Transaction(nil)
	assert.True(queue.ErrTxNotSupported.Is(err))
}

func (s *QueueSuite) TestRetryQueue() {
	assert := assert.New(s.T())

	qName := NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	// 1: Publish jobs to the main queue.
	j1, err := queue.NewJob()
	assert.NoError(err)
	err = j1.Encode(1)
	assert.NoError(err)

	err = q.Publish(j1)
	assert.NoError(err)

	j2, err := queue.NewJob()
	assert.NoError(err)
	err = j2.Encode(2)
	assert.NoError(err)
	err = q.Publish(j2)
	assert.NoError(err)

	// 2: consume and reject them.
	advertisedWindow := 1
	iterMain, err := q.Consume(advertisedWindow)
	assert.NoError(err)
	assert.NotNil(iterMain)

	jReject1, err := iterMain.Next()
	assert.NoError(err)
	assert.NotNil(jReject1)
	// Jobs should go to the retry queue when rejected with requeue = false
	err = jReject1.Reject(false)
	assert.NoError(err)

	jReject2, err := iterMain.Next()
	assert.NoError(err)
	assert.NotNil(jReject2)
	err = jReject2.Reject(false)
	assert.NoError(err)

	// 3. republish the jobs in the retry queue.
	err = q.RepublishBuried()
	assert.NoError(err)

	// 4. re-read the jobs on the main queue.
	var payload int
	jRepub1, err := iterMain.Next()
	assert.NoError(jRepub1.Decode(&payload))
	assert.Equal(1, payload)
	assert.NoError(jRepub1.Ack())

	jRepub2, err := iterMain.Next()
	assert.NoError(jRepub2.Decode(&payload))
	assert.Equal(2, payload)
	assert.NoError(jRepub2.Ack())

	done := s.checkNextClosed(iterMain)
	assert.NoError(iterMain.Close())
	iterMain.Close()
	<-done
}

func (s *QueueSuite) TestConcurrent() {
	testCases := []int{1, 2, 13, 150}

	for _, advertisedWindow := range testCases {
		s.T().Run(strconv.Itoa(advertisedWindow), func(t *testing.T) {
			assert := assert.New(t)

			qName := NewName()
			q, err := s.Broker.Queue(qName)
			assert.NoError(err)
			assert.NotNil(q)

			var continueWG sync.WaitGroup
			continueWG.Add(1)

			var calledWG sync.WaitGroup

			var calls int32
			atomic.StoreInt32(&calls, 0)

			iter, err := q.Consume(advertisedWindow)
			assert.NoError(err)

			go func() {
				for {
					j, err := iter.Next()
					if queue.ErrAlreadyClosed.Is(err) {
						return
					}
					assert.NoError(err)
					if j == nil {
						time.Sleep(300 * time.Millisecond)
						continue
					}

					go func() {
						// Removes 1 from calledWG, and gets locked
						// until continueWG is released
						atomic.AddInt32(&calls, 1)

						calledWG.Done()
						continueWG.Wait()

						assert.NoError(j.Ack())
					}()
				}
			}()

			assert.EqualValues(0, atomic.LoadInt32(&calls))
			calledWG.Add(advertisedWindow)

			// Enqueue some jobs, 3 * advertisedWindow
			for i := 0; i < advertisedWindow*3; i++ {
				j, err := queue.NewJob()
				assert.NoError(err)
				err = j.Encode(i)
				assert.NoError(err)
				err = q.Publish(j)
				assert.NoError(err)
			}

			// The first batch of calls should be exactly advertisedWindow
			calledWG.Wait()
			assert.EqualValues(advertisedWindow, atomic.LoadInt32(&calls))

			// Let the iterator go though all the jobs, should be 3*advertisedWindow
			calledWG.Add(2 * advertisedWindow)
			continueWG.Done()
			calledWG.Wait()
			assert.EqualValues(3*advertisedWindow, atomic.LoadInt32(&calls))
		})
	}
}

func (s *QueueSuite) checkNextClosed(iter queue.JobIter) chan struct{} {
	assert := assert.New(s.T())

	done := make(chan struct{})
	go func() {
		j, err := iter.Next()
		assert.True(queue.ErrAlreadyClosed.Is(err))
		assert.Nil(j)
		done <- struct{}{}
	}()
	return done
}
