package amqp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"gopkg.in/src-d/go-queue.v1"
	"gopkg.in/src-d/go-queue.v1/test"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// RabbitMQ reconnect tests require running docker.
// If `docker ps` command returned an error we skip some of the tests.
var (
	dockerIsRunning bool
	dockerCmdOutput string
	inAppVeyor      bool
)

func init() {
	cmd := exec.Command("docker", "ps")
	b, err := cmd.CombinedOutput()

	dockerCmdOutput, dockerIsRunning = string(b), (err == nil)
	inAppVeyor = os.Getenv("APPVEYOR") == "True"
}

func TestAMQPSuite(t *testing.T) {
	suite.Run(t, new(AMQPSuite))
}

type AMQPSuite struct {
	test.QueueSuite
}

const testAMQPURI = "amqp://127.0.0.1:5672"

func (s *AMQPSuite) SetupSuite() {
	s.BrokerURI = testAMQPURI
}

func TestDefaultConfig(t *testing.T) {
	assert.Equal(t, DefaultConfiguration.BuriedExchangeSuffix, ".buriedExchange")
}

func TestNewAMQPBroker_bad_url(t *testing.T) {
	assert := assert.New(t)

	b, err := New("badurl")
	assert.Error(err)
	assert.Nil(b)
}

func sendJobs(assert *assert.Assertions, n int, p queue.Priority, q queue.Queue) {
	for i := 0; i < n; i++ {
		j, err := queue.NewJob()
		assert.NoError(err)
		j.SetPriority(p)
		err = j.Encode(i)
		assert.NoError(err)
		err = q.Publish(j)
		assert.NoError(err)
	}
}

func TestAMQPPriorities(t *testing.T) {
	assert := assert.New(t)

	broker, err := New(testAMQPURI)
	assert.NoError(err)
	if !assert.NotNil(broker) {
		return
	}

	name := test.NewName()
	q, err := broker.Queue(name)
	assert.NoError(err)
	assert.NotNil(q)

	// Send 50 low priority jobs
	sendJobs(assert, 50, queue.PriorityLow, q)

	// Send 50 high priority jobs
	sendJobs(assert, 50, queue.PriorityUrgent, q)

	// Receive and collect priorities
	iter, err := q.Consume(1)
	assert.NoError(err)
	assert.NotNil(iter)

	sumFirst := uint(0)
	sumLast := uint(0)

	for i := 0; i < 100; i++ {
		j, err := iter.Next()
		assert.NoError(err)
		assert.NoError(j.Ack())

		if i < 50 {
			sumFirst += uint(j.Priority)
		} else {
			sumLast += uint(j.Priority)
		}
	}

	assert.True(sumFirst > sumLast)
	assert.Equal(uint(queue.PriorityUrgent)*50, sumFirst)
	assert.Equal(uint(queue.PriorityLow)*50, sumLast)
}

func TestAMQPHeaders(t *testing.T) {
	broker, err := queue.NewBroker(testAMQPURI)
	require.NoError(t, err)
	defer func() { require.NoError(t, broker.Close()) }()

	q, err := broker.Queue(test.NewName())
	require.NoError(t, err)

	tests := []struct {
		name      string
		retries   int32
		errorType string
	}{
		{
			name: fmt.Sprintf("with %s and %s headers",
				DefaultConfiguration.RetriesHeader, DefaultConfiguration.ErrorHeader),
			retries:   int32(10),
			errorType: "error-test",
		},
		{
			name:      fmt.Sprintf("with %s header", DefaultConfiguration.RetriesHeader),
			retries:   int32(10),
			errorType: "",
		},
		{
			name:      fmt.Sprintf("with %s headers", DefaultConfiguration.ErrorHeader),
			retries:   int32(0),
			errorType: "error-test",
		},
		{
			name:      "with no headers",
			retries:   int32(0),
			errorType: "",
		},
	}

	for i, test := range tests {
		job, err := queue.NewJob()
		require.NoError(t, err)

		job.Retries = test.retries
		job.ErrorType = test.errorType

		require.NoError(t, job.Encode(i))
		require.NoError(t, q.Publish(job))
	}

	jobIter, err := q.Consume(len(tests))
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, err := jobIter.Next()
			require.NoError(t, err)
			require.NotNil(t, job)

			require.Equal(t, test.retries, job.Retries)
			require.Equal(t, test.errorType, job.ErrorType)
		})
	}
}

func TestAMQPHeaderRetriesType(t *testing.T) {
	broker, err := queue.NewBroker(testAMQPURI)
	require.NoError(t, err)
	defer func() { require.NoError(t, broker.Close()) }()

	q, err := broker.Queue(test.NewName())
	require.NoError(t, err)

	qa, ok := q.(*Queue)
	require.True(t, ok)

	tests := []struct {
		name    string
		retries interface{}
	}{
		{
			name:    "int16",
			retries: int16(42),
		},
		{
			name:    "int32",
			retries: int32(42),
		},
		{
			name:    "int64",
			retries: int64(42),
		},
	}

	for _, test := range tests {
		headers := amqp.Table{}
		headers[DefaultConfiguration.RetriesHeader] = test.retries
		err := qa.conn.channel().Publish(
			"",            // exchange
			qa.queue.Name, // routing key
			false,         // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				MessageId:    "id",
				Priority:     uint8(queue.PriorityNormal),
				Timestamp:    time.Now(),
				ContentType:  "application/msgpack",
				Body:         []byte("gaxSZXBvc2l0b3J5SUTEEAFmXSlGxxOsFGMLs/gl7Qw="),
				Headers:      headers,
			},
		)
		require.NoError(t, err)
	}

	jobIter, err := q.Consume(len(tests))
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, err := jobIter.Next()
			require.NoError(t, err)
			require.NotNil(t, job)

			require.Equal(t, int32(42), job.Retries)
		})
	}
}

func TestAMQPRepublishBuried(t *testing.T) {
	broker, err := queue.NewBroker(testAMQPURI)
	require.NoError(t, err)
	defer func() { require.NoError(t, broker.Close()) }()

	queueName := test.NewName()
	q, err := broker.Queue(queueName)
	require.NoError(t, err)

	amqpQueue, ok := q.(*Queue)
	require.True(t, ok)

	buried := amqpQueue.buriedQueue

	tests := []struct {
		name    string
		payload string
	}{
		{name: "message 1", payload: "payload 1"},
		{name: "message 2", payload: "republish"},
		{name: "message 3", payload: "payload 3"},
		{name: "message 3", payload: "payload 4"},
	}

	for _, utest := range tests {
		job, err := queue.NewJob()
		require.NoError(t, err)

		job.Raw = []byte(utest.payload)

		err = buried.Publish(job)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
	}

	var condition queue.RepublishConditionFunc = func(j *queue.Job) bool {
		return string(j.Raw) == "republish"
	}

	err = q.RepublishBuried(condition)
	require.NoError(t, err)

	jobIter, err := q.Consume(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, jobIter.Close()) }()

	job, err := jobIter.Next()
	require.NoError(t, err)
	require.Equal(t, string(job.Raw), "republish")
}

func TestReconnect(t *testing.T) {
	if inAppVeyor || !dockerIsRunning {
		t.Skip()
	}

	broker, err := queue.NewBroker(testAMQPURI)
	require.NoError(t, err)
	defer func() { broker.Close() }()

	queueName := test.NewName()
	q, err := broker.Queue(queueName)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rabbitHiccup(ctx, 5*time.Second)

	tests := []struct {
		name    string
		payload string
	}{
		{name: "message 1", payload: "payload 1"},
		{name: "message 2", payload: "payload 2"},
		{name: "message 3", payload: "payload 3"},
		{name: "message 3", payload: "payload 4"},
	}

	for _, test := range tests {
		job, err := queue.NewJob()
		require.NoError(t, err)

		job.Raw = []byte(test.payload)

		err = q.Publish(job)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	n := len(tests)
	jobIter, err := q.Consume(1)
	require.NoError(t, err)
	defer func() { jobIter.Close() }()

	for i := 0; i < n; i++ {
		if job, err := jobIter.Next(); err != nil {
			t.Log(err)

			job, err = queue.NewJob()
			require.NoError(t, err)
			job.Raw = []byte("check connection - retry till we connect")
			err = q.Publish(job)
			require.NoError(t, err)
			break
		} else {
			t.Log(string(job.Raw))
		}
	}
}

// rabbitHiccup restarts rabbitmq every interval
// it requires the RabbitMQ running in docker container:
// docker run --name rabbitmq -d -p 127.0.0.1:5672:5672 rabbitmq:3-management
func rabbitHiccup(ctx context.Context, interval time.Duration) error {
	cmd := exec.Command("docker", "restart", "rabbitmq")
	err := cmd.Start()
	for err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()

		case <-time.After(interval):
			err = cmd.Start()
		}
	}

	return err
}
