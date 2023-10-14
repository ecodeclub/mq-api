package mqerr

import "errors"

var (
	ErrTopicNotFound   = errors.New("topic未找到")
	ErrKafkaUnKnowType = errors.New("未知的kafka消息类型")
)
