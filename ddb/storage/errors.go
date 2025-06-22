package storage

import "errors"

// Shared errors across operations
var (
	RateLimitReachedError = errors.New("rate limit reached")
	ErrUnprocessed        = errors.New("unprocessed entry")
)

type ConditionalCheckFailedException struct {
	Message string
}

func (e *ConditionalCheckFailedException) Error() string {
	return e.Message
}
