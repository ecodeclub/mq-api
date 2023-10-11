package memory

import (
	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"sync"
)

type Mq struct {
	topics syncx.Map[string, *Topic]
}

func NewMq() mq.MQ {
	return &Mq{
		syncx.Map[string, *Topic]{},
	}
}

type Topic struct {
	Name        string
	lock        sync.RWMutex
	consumerChs map[string]chan *mq.Message
	produceChan chan *mq.Message
}

func NewTopic(name string, opts ...topicOption) *Topic {
	t := &Topic{
		Name:        name,
		produceChan: make(chan *mq.Message, 1000),
		consumerChs: make(map[string]chan *mq.Message),
	}
	for _, opt := range opts {
		opt(t)
	}
	go func() {
		for msg := range t.produceChan {
			t.lock.RLock()
			consumers := t.consumerChs
			t.lock.RUnlock()
			for _, ch := range consumers {
				ch <- msg
			}
		}
	}()
	return t
}

type topicOption func(topic *Topic)

func WithProducerChannelSize(size int) topicOption {
	return func(topic *Topic) {
		topic.produceChan = make(chan *mq.Message, size)
	}
}

func (t *Topic) NewConsumer(size int, id string) mq.Consumer {
	t.lock.Lock()
	defer t.lock.Unlock()
	ch, ok := t.consumerChs[id]
	if !ok {
		ch = make(chan *mq.Message, size)
		t.consumerChs[id] = ch
	}

	return &TopicConsumer{
		ConsumerCh: ch,
	}
}

func (t *Topic) NewProducer(topic string) mq.Producer {
	return &TopicProducer{
		ProducerCh: t.produceChan,
		topic:      topic,
	}
}

func (m *Mq) Topic(name string, partition int) error {
	NewTopic(name)
	return nil
}

func (m *Mq) Close() error {
	return nil
}

func (m *Mq) Consumer(topic string, id string) (mq.Consumer, error) {
	tp, _ := m.topics.LoadOrStore(topic, NewTopic(topic))
	return tp.NewConsumer(10, id), nil
}

func (m *Mq) Producer(topic string) (mq.Producer, error) {
	tp, _ := m.topics.LoadOrStore(topic, NewTopic(topic))
	return tp.NewProducer(topic), nil
}
