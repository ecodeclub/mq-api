package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
)

const (
	defaultProducerChanSize = 1000
	defaultConsumerChanSize = 10
)

func NewMQ() mq.MQ {
	return &queue{
		syncx.Map[string, *topic]{},
	}
}

type queue struct {
	topics syncx.Map[string, *topic]
}

func (q *queue) CreateTopic(ctx context.Context, topic string, partitions int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, loaded := q.topics.LoadOrStore(topic, newTopic(topic, partitions))
	if loaded {
		return errors.New("topic已存在")
	}
	return nil
}

func (q *queue) DeleteTopics(ctx context.Context, topics ...string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, topic := range topics {
		q.topics.Delete(topic)
	}
	return nil
}

func (q *queue) Producer(topic string) (mq.Producer, error) {
	tp, ok := q.topics.Load(topic)
	if !ok {
		return nil, errors.New("未知topic")
	}
	return tp.newProducer(topic), nil
}

func (q *queue) Consumer(topic, groupID string) (mq.Consumer, error) {
	tp, ok := q.topics.Load(topic)
	if !ok {
		return nil, errors.New("未创建topic")
	}
	return tp.newConsumer(defaultConsumerChanSize, groupID), nil
}

func (q *queue) Close() error {
	return nil
}

type topic struct {
	Name       string
	Partitions int
	lock       sync.RWMutex
	consumers  []chan *mq.Message
	producer   chan *mq.Message
}

func (t *topic) newConsumer(chanSize int, groupID string) mq.Consumer {
	consumerChan := make(chan *mq.Message, chanSize)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.consumers = append(t.consumers, consumerChan)
	return &Consumer{
		messageChan: consumerChan,
		groupID:     groupID,
	}
}

func (t *topic) newProducer(topic string) mq.Producer {
	return &Producer{
		ProducerCh: t.producer,
		topic:      topic,
	}
}

func newTopic(name string, partitions int) *topic {
	t := &topic{
		Name:       name,
		Partitions: partitions,
		producer:   make(chan *mq.Message, defaultProducerChanSize),
	}
	go func() {
		for msg := range t.producer {
			t.lock.RLock()
			consumers := t.consumers
			t.lock.RUnlock()
			for _, ch := range consumers {
				ch <- msg
			}
		}
	}()
	return t
}
