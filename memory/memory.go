package memory

import (
	"io"
	"sync"
	"time"

	"github.com/go-mq/mq/v2"
)

func init() {
	mq.Register("memory", func(uri string) (mq.Broker, error) {
		return New(), nil
	})

	mq.Register("memoryfinite", func(uri string) (mq.Broker, error) {
		return NewFinite(true), nil
	})
}

// Broker is a in-memory implementation of Broker.
type Broker struct {
	queues map[string]mq.Queue
	finite bool
}

// New creates a new Broker for an in-memory queue.
func New() mq.Broker {
	return NewFinite(false)
}

// NewFinite creates a new Broker for an in-memory queue. The argument
// specifies if the JobIter stops on EOF or not.
func NewFinite(finite bool) mq.Broker {
	return &Broker{
		queues: make(map[string]mq.Queue),
		finite: finite,
	}
}

// Queue returns the queue with the given name.
func (b *Broker) Queue(name string) (mq.Queue, error) {
	if _, ok := b.queues[name]; !ok {
		b.queues[name] = &Queue{
			jobs:   make([]*mq.Job, 0, 10),
			finite: b.finite,
		}
	}

	return b.queues[name], nil
}

// Close closes the connection in the Broker.
func (b *Broker) Close() error {
	return nil
}

// Queue implements a queue.Queue interface.
type Queue struct {
	jobs       []*mq.Job
	buriedJobs []*mq.Job
	sync.RWMutex
	idx                int
	publishImmediately bool
	finite             bool
}

// Publish publishes a Job to the queue.
func (q *Queue) Publish(j *mq.Job) error {
	if j == nil || j.Size() == 0 {
		return mq.ErrEmptyJob.New()
	}

	q.Lock()
	defer q.Unlock()
	q.jobs = append(q.jobs, j)
	return nil
}

// PublishDelayed publishes a Job to the queue with a given delay.
func (q *Queue) PublishDelayed(j *mq.Job, delay time.Duration) error {
	if j == nil || j.Size() == 0 {
		return mq.ErrEmptyJob.New()
	}

	if q.publishImmediately {
		return q.Publish(j)
	}
	go func() {
		time.Sleep(delay)
		q.Publish(j)
	}()
	return nil
}

// RepublishBuried implements the Queue interface.
func (q *Queue) RepublishBuried(conditions ...mq.RepublishConditionFunc) error {
	for _, job := range q.buriedJobs {
		if mq.RepublishConditions(conditions).Comply(job) {
			job.ErrorType = ""
			if err := q.Publish(job); err != nil {
				return err
			}
		}
	}
	return nil
}

// Transaction calls the given callback inside a transaction.
func (q *Queue) Transaction(txcb mq.TxCallback) error {
	txQ := &Queue{jobs: make([]*mq.Job, 0, 10), publishImmediately: true}
	if err := txcb(txQ); err != nil {
		return err
	}

	q.jobs = append(q.jobs, txQ.jobs...)
	return nil
}

// Consume implements Queue. The advertisedWindow value is the maximum number of
// unacknowledged jobs. Use 0 for an infinite window.
func (q *Queue) Consume(advertisedWindow int) (mq.JobIter, error) {
	jobIter := JobIter{
		q:       q,
		RWMutex: &q.RWMutex,
		finite:  q.finite,
	}

	if advertisedWindow > 0 {
		jobIter.chn = make(chan struct{}, advertisedWindow)
	}

	return &jobIter, nil
}

// JobIter implements a queue.JobIter interface.
type JobIter struct {
	q      *Queue
	closed bool
	finite bool
	chn    chan struct{}
	*sync.RWMutex
}

// Acknowledger implements a queue.Acknowledger interface.
type Acknowledger struct {
	q   *Queue
	j   *mq.Job
	chn chan struct{}
}

// Ack is called when the Job has finished.
func (a *Acknowledger) Ack() error {
	a.release()
	return nil
}

// Reject is called when the Job has errored. The argument indicates whether the Job
// should be put back in queue or not.  If requeue is false, the job will go to the buried
// queue until Queue.RepublishBuried() is called.
func (a *Acknowledger) Reject(requeue bool) error {
	defer a.release()

	if !requeue {
		// Send to the buried queue for later republishing
		a.q.buriedJobs = append(a.q.buriedJobs, a.j)
		return nil
	}

	return a.q.Publish(a.j)
}

func (a *Acknowledger) release() {
	if a.chn != nil {
		<-a.chn
	}
}

func (i *JobIter) isClosed() bool {
	i.RLock()
	defer i.RUnlock()
	return i.closed
}

// Next returns the next job in the iter.
func (i *JobIter) Next() (*mq.Job, error) {
	i.acquire()
	for {
		if i.isClosed() {
			i.release()
			return nil, mq.ErrAlreadyClosed.New()
		}

		j, err := i.next()
		if err == nil {
			return j, nil
		}

		if err == io.EOF && i.finite {
			i.release()
			return nil, err
		}

		time.Sleep(1 * time.Second)
	}
}

func (i *JobIter) next() (*mq.Job, error) {
	i.Lock()
	defer i.Unlock()
	if len(i.q.jobs) <= i.q.idx {
		return nil, io.EOF
	}

	j := i.q.jobs[i.q.idx]
	j.Acknowledger = &Acknowledger{j: j, q: i.q, chn: i.chn}
	i.q.idx++

	return j, nil
}

// Close closes the iter.
func (i *JobIter) Close() error {
	i.Lock()
	defer i.Unlock()
	i.closed = true
	return nil
}

func (i *JobIter) acquire() {
	if i.chn != nil {
		i.chn <- struct{}{}
	}
}

func (i *JobIter) release() {
	if i.chn != nil {
		<-i.chn
	}
}
