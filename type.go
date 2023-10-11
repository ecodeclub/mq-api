package mq

import "context"

type Header map[string]string

type Message struct {
	Value  []byte
	Key    []byte
	Header Header
	Topic  string
	// 分区
	Partition int64
	// 偏移量
	Offset int64
}

type ProducerResult struct {
}

type Producer interface {
	Produce(ctx context.Context, m *Message) (*ProducerResult, error)
}

type Consumer interface {
	Consume(ctx context.Context) (*Message, error)
	ConsumeMsgCh(ctx context.Context) (<-chan *Message, error)
	ConsumerSeek(partition int64, offset int64) error
}

type MQ interface {
	Topic(name string, partition int) error
	Producer(topic string) (Producer, error)
	Consumer(topic string, id string) (Consumer, error)
	Close() error
}
