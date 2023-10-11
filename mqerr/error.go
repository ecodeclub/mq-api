package mqerr

import "errors"

var (
	ErrTopicNotFound = errors.New("topic未找到")
)
