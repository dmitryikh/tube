package api

import (
	"fmt"

	"github.com/dmitryikh/tube"
)

type ErrorCode uint64

const (
	// CodeOk           ErrorCode = 0
	CodeTopicExists  ErrorCode = 1
	CodeUnknownError ErrorCode = 1000
)

func NewTopicExistsError(topicName string) *Error {
	return &Error{
		Code:    uint64(CodeTopicExists),
		Message: fmt.Sprintf("Topic \"%s\" already exists", topicName),
	}
}

func NewUnknownError(err error) *Error {
	return &Error{
		Code:    uint64(CodeUnknownError),
		Message: err.Error(),
	}
}

func ProtoErrorFromError(err error) *Error {
	switch e := err.(type) {
	case tube.TopicExistsError:
		return NewTopicExistsError(e.TopicName)
	default:
		return NewUnknownError(e)
	}
}

func ErrorFromProtoError(e *Error) error {
	errorCode := ErrorCode(e.Code)
	switch errorCode {
	// case CodeOk:
	// 	return nil
	case CodeTopicExists:
		// TODO: topicName???
		return tube.NewTopicExistsError("")
	default:
		return fmt.Errorf("%s", e.Message)
	}
}
