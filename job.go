package mq

import (
	"encoding"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v4"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/yaml.v2"
	"time"
)

const (
	ContentTypeMsgpack  = "application/msgpack"
	ContentTypeJSON     = "application/json"
	ContentTypeYAML     = "application/yaml"
	ContentTypeProtobuf = "application/protobuf"
)

// Job contains the information for a job to be published to a queue.
type Job struct {
	// ID of the job.
	ID string
	// Priority is the priority level.
	Priority Priority
	// Timestamp is the time of creation.
	Timestamp time.Time
	// Retries is the number of times this job can be processed before being rejected.
	Retries int32
	// ErrorType is the kind of error that made the job fail.
	ErrorType string
	// ContentType of the job
	ContentType string
	// Raw content of the Job
	Raw []byte
	// Acknowledger is the acknowledgement management system for the job.
	Acknowledger Acknowledger
}

// Acknowledger represents the object in charge of acknowledgement
// management for a job. When a job is acknowledged using any of the
// functions in this interface, it will be considered delivered by the
// queue.
type Acknowledger interface {
	// Ack is called when the Job has finished.
	Ack() error
	// Reject is called if the job has errored. The parameter indicates
	// whether the job should be put back in the queue or not.
	Reject(requeue bool) error
}

// NewJob creates a new Job with default values, a new unique ID and current
// timestamp.
func NewJob() *Job {
	return &Job{
		ID:          uuid.New().String(),
		Priority:    PriorityNormal,
		Timestamp:   time.Now(),
		ContentType: ContentTypeMsgpack,
	}
}

// SetPriority sets job priority
func (j *Job) SetPriority(priority Priority) {
	j.Priority = priority
}

// Encode encodes the payload to the wire format used.
func (j *Job) Encode(payload interface{}) error {
	var err error
	j.Raw, err = encode(ContentTypeMsgpack, &payload)
	if err != nil {
		return err
	}

	return nil
}

// Decode decodes the payload from the wire format.
func (j *Job) Decode(payload interface{}) error {
	return decode(ContentTypeMsgpack, j.Raw, &payload)
}

// ErrCantAck is the error returned when the Job does not come from a queue
var ErrCantAck = errors.NewKind("can't acknowledge this message, it does not come from a queue")

// Ack is called when the job is finished.
func (j *Job) Ack() error {
	if j.Acknowledger == nil {
		return ErrCantAck.New()
	}
	return j.Acknowledger.Ack()
}

// Reject is called when the job errors. The parameter is true if and only if the
// job should be put back in the queue.
func (j *Job) Reject(requeue bool) error {
	if j.Acknowledger == nil {
		return ErrCantAck.New()
	}
	return j.Acknowledger.Reject(requeue)
}

// Size returns the size of the message.
func (j *Job) Size() int {
	return len(j.Raw)
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

func encode(mime string, p interface{}) ([]byte, error) {
	switch mime {
	case ContentTypeMsgpack:
		return msgpack.Marshal(p)
	case ContentTypeJSON:
		return json.Marshal(p)
	case ContentTypeYAML:
		return yaml.Marshal(p)
	case ContentTypeProtobuf:
		var ok bool
		var pm proto.Message

		if pm, ok = p.(proto.Message); !ok {
			return nil, fmt.Errorf("must provide payload that implements proto.Message interface")
		}

		return proto.Marshal(pm)
	default:
		// No recognized mime-type, try encoding interfaces or error out
		switch p.(type) {
		case encoding.BinaryMarshaler:
			return p.(encoding.BinaryMarshaler).MarshalBinary()
		case encoding.TextMarshaler:
			return p.(encoding.TextMarshaler).MarshalText()
		case Marshaler:
			return p.(Marshaler).Marshal()
		}

		return nil, fmt.Errorf("unknown content type: %s", mime)
	}
}

func decode(mime string, r []byte, p interface{}) error {
	switch mime {
	case ContentTypeMsgpack:
		return msgpack.Unmarshal(r, p)
	case ContentTypeJSON:
		return json.Unmarshal(r, p)
	case ContentTypeYAML:
		return yaml.Unmarshal(r, p)
	case ContentTypeProtobuf:
		var ok bool
		var pm proto.Message

		if pm, ok = p.(proto.Message); !ok {
			return fmt.Errorf("must provide payload that implements proto.Message interface")
		}

		return proto.Unmarshal(r, pm)
	default:
		// No recognized mime-type, try decoding interfaces or error out
		switch p.(type) {
		case encoding.BinaryUnmarshaler:
			return p.(encoding.BinaryUnmarshaler).UnmarshalBinary(r)
		case encoding.TextUnmarshaler:
			return p.(encoding.TextUnmarshaler).UnmarshalText(r)
		case Unmarshaler:
			return p.(Unmarshaler).Unmarshal(r)
		}

		return fmt.Errorf("unknown content type: %s", mime)
	}
}
