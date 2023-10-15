package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/mqerr"
	"go.uber.org/multierr"
	"strings"
	"sync"
)

// 先默认1000
const msgChannelSize = 1000

type MQ struct {
	// 用于创建topic
	adminClient *kafka.AdminClient
	brokers     []string
	locker      sync.RWMutex
	closed      bool
	// 方便释放资源
	producers []mq.Producer
	consumers []mq.Consumer
}

func NewMQ(brokers []string) (mq.MQ, error) {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
	})
	if err != nil {
		return nil, err
	}
	return &MQ{
		adminClient: a,
		brokers:     brokers,
	}, nil
}

// ClearTopic 删除topic，仅供测试
func (m *MQ) ClearTopic(ctx context.Context, topics []string) error {
	if m.isClosed() {
		return mqerr.ErrMqIsClosed
	}
	res, err := m.adminClient.DeleteTopics(ctx, topics)
	if err != nil {
		return err
	}
	errList := make([]error, 0, len(res))
	for _, r := range res {
		if r.Error.Code() != kafka.ErrUnknownTopicOrPart && r.Error.Code() != kafka.ErrNoError {
			errList = append(errList, r.Error)
		}
	}
	return multierr.Combine(errList...)
}

func (m *MQ) Topic(ctx context.Context, name string, partitions int) error {
	if m.isClosed() {
		return mqerr.ErrMqIsClosed
	}
	res, err := m.adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{
			{
				Topic:         name,
				NumPartitions: partitions,
			},
		},
		kafka.SetAdminOperationTimeout(0),
	)
	if res[0].Error.Code() != 0 {
		return res[0].Error
	}
	return err

}

func (m *MQ) Producer(topic string) (mq.Producer, error) {
	if m.isClosed() {
		return nil, mqerr.ErrMqIsClosed
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(m.brokers, ","),
	})
	if err != nil {
		return nil, err
	}
	customProducer := &Producer{
		topic:    topic,
		producer: p,
	}
	m.locker.Lock()
	m.producers = append(m.producers, customProducer)
	m.locker.Unlock()
	return customProducer, nil
}

func (m *MQ) Consumer(topic string, id string) (mq.Consumer, error) {
	if m.isClosed() {
		return nil, mqerr.ErrMqIsClosed
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(m.brokers, ","),
		"group.id":          id,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}
	msgCh := make(chan *mq.Message, msgChannelSize)
	customConsumer := &Consumer{
		id:       id,
		topic:    topic,
		consumer: c,
		closeCh:  make(chan struct{}),
		msgCh:    msgCh,
	}
	m.locker.Lock()
	m.consumers = append(m.consumers, customConsumer)
	m.locker.Unlock()

	go customConsumer.getMsgFromKafka()
	return customConsumer, nil
}

func (m *MQ) Close() error {
	m.locker.Lock()
	defer m.locker.Unlock()
	errorList := make([]error, 0, len(m.consumers))
	for _, p := range m.producers {
		err := p.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	for _, c := range m.consumers {
		err := c.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	m.closed = true
	m.adminClient.Close()
	return multierr.Combine(errorList...)
}

func (m *MQ) isClosed() bool {
	m.locker.RLock()
	defer m.locker.RUnlock()
	return m.closed
}
