package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ecodeclub/mq-api"
	"go.uber.org/multierr"
	"strings"
	"sync"
)

// 先默认1000
const msgChannelSize = 1000

type CustomMQ struct {
	// 用于创建topic
	adminClient *kafka.AdminClient
	brokers     []string
	locker      sync.RWMutex
	// 方便释放资源
	producers []mq.Producer
	consumers []mq.Consumer
}

func NewCustomMq(brokers []string) (*CustomMQ, error) {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
	})
	if err != nil {
		return nil, err
	}
	return &CustomMQ{
		adminClient: a,
		brokers:     brokers,
	}, nil
}

// ClearKafka 删除topic，仅供测试
func (m *CustomMQ) ClearKafka(topics []string) error {
	_, err := m.adminClient.DeleteTopics(context.Background(), topics)
	return err
}

func (m *CustomMQ) Topic(ctx context.Context, name string, partition int) error {
	res, err := m.adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{
			{
				Topic:         name,
				NumPartitions: partition,
			},
		},
		kafka.SetAdminOperationTimeout(0),
	)
	if res[0].Error.Code() != 0 {
		return res[0].Error
	}
	return err

}

func (m *CustomMQ) Producer(topic string) (mq.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(m.brokers, ","),
	})
	if err != nil {
		return nil, err
	}
	customProducer := &CustomProducer{
		topic:    topic,
		producer: p,
	}
	m.locker.Lock()
	m.producers = append(m.producers, customProducer)
	m.locker.Unlock()
	return customProducer, nil
}

func (m *CustomMQ) Consumer(topic string, id string) (mq.Consumer, error) {
	// Create and configure a Kafka consumer here
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
	customConsumer := &CustomConsumer{
		id:       id,
		topic:    topic,
		consumer: c,
		closeCh:  make(chan struct{}),
		msgCh:    msgCh,
	}
	m.locker.Lock()
	m.consumers = append(m.consumers, customConsumer)
	m.locker.Unlock()

	go customConsumer.GetMsgFromKafka()
	return customConsumer, nil
}

func (m *CustomMQ) Close() error {
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
	m.adminClient.Close()
	return multierr.Combine(errorList...)
}
