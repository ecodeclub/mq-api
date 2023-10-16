package mqerr

import "errors"

var (
	ErrTopicNotFound     = errors.New("topic未找到")
	ErrUnKnowMessageType = errors.New("未知的消息类型")
	ErrConsumerIsClosed  = errors.New("消费者已经关闭")
	ErrProducerIsClosed  = errors.New("生产者已经关闭")
	ErrMQIsClosed        = errors.New("mq已经关闭")
)
