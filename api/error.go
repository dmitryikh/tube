package api

import (
	"fmt"

	"github.com/dmitryikh/tube"
)

type ErrorCode uint64

const (
	// CodeOk           ErrorCode = 0
	CodeTopicExists    ErrorCode = 1
	CodeTopicNotExists ErrorCode = 2
	CodeTopicEmpty     ErrorCode = 3

	CodeConsumerExists    ErrorCode = 100
	CodeConsumerNotExists ErrorCode = 101
	CodeUnknownError      ErrorCode = 1000
)

func ProtoErrorFromError(err error) *Error {
	switch e := err.(type) {
	case tube.TopicExistsError:
		return &Error{
			Code: uint64(CodeTopicExists),
			Data: map[string]string{
				"topic": e.TopicName,
			},
		}
	case tube.TopicNotExistError:
		return &Error{
			Code: uint64(CodeTopicNotExists),
			Data: map[string]string{
				"topic": e.TopicName,
			},
		}
	case tube.TopicEmptyError:
		return &Error{
			Code: uint64(CodeTopicEmpty),
			Data: map[string]string{
				"topic": e.TopicName,
			},
		}
	case tube.ConsumerExistsError:
		return &Error{
			Code: uint64(CodeConsumerExists),
			Data: map[string]string{
				"consumer": e.ConsumerID,
			},
		}
	default:
		return &Error{
			Code: uint64(CodeUnknownError),
			Data: map[string]string{
				"message": e.Error(),
			},
		}
	}
}

func ErrorFromProtoError(e *Error) error {
	errorCode := ErrorCode(e.Code)
	switch errorCode {
	// case CodeOk:
	// 	return nil
	case CodeTopicExists:
		return tube.NewTopicExistsError(e.Data["topic"])
	case CodeTopicNotExists:
		return tube.NewTopicNotExistError(e.Data["topic"])
	case CodeTopicEmpty:
		return tube.NewTopicEmptyError(e.Data["topic"])
	case CodeConsumerExists:
		return tube.NewConsumerExistsError(e.Data["consumer"])
	default:
		return fmt.Errorf("%s", e.Data["message"])
	}
}
