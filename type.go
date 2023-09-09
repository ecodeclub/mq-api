package mq

import "context"

type Header map[string]string

type Message struct {
	Value  []byte
	Key    []byte
	Header Header
	Topic  string
}

type ProducerResult struct{}

type Producer interface {
	Produce(ctx context.Context, m *Message) (*ProducerResult, error)
}

type Consumer interface {
	Consume(ctx context.Context) (*Message, error)
	ConsumeMsgCh(ctx context.Context) (<-chan *Message, error)
}

type MQ interface {
	Producer(topic string) Producer
	Consumer(topic string) Consumer
}
