package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ecodeclub/mq-api"
	"go.uber.org/multierr"
	"strings"
	"sync"
)

type CustomMQ struct {
	brokers   []string
	locker    sync.RWMutex
	producers []*kafka.Producer
	consumers []*kafka.Consumer
}

func (m *CustomMQ) Topic(name string, partition int) error {
	return nil
}

func (m *CustomMQ) Producer(topic string) (mq.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(m.brokers, ","),
		"topic":             topic,
	})
	if err != nil {
		return nil, err
	}

	return &CustomProducer{producer: p}, nil
}

func (m *CustomMQ) Consumer(topic string, id string) (mq.Consumer, error) {
	// Create and configure a Kafka consumer here
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(m.brokers, ","),
		"group.id":          id,
		"auto.offset.reset": "latest",
	})
	if err != nil {
		return nil, err
	}
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}
	return &CustomConsumer{consumer: c}, nil
}

func (m *CustomMQ) Close() error {
	m.locker.Lock()
	defer m.locker.Unlock()
	errorList := make([]error, 0, len(m.consumers))
	for _, p := range m.producers {
		p.Close()
	}
	for _, c := range m.consumers {
		err := c.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}
