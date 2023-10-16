package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka/common"
	"github.com/ecodeclub/mq-api/mqerr"
	"sync"
)

type Producer struct {
	topic    string
	producer *kafka.Producer
	closed   bool
	locker   sync.RWMutex
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	kafkaMsg := &kafka.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: common.ConvertHeaderMap(m.Header),
		TopicPartition: kafka.TopicPartition{
			Topic: &p.topic,
		},
	}
	return p.produce(ctx, kafkaMsg)
}

func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partition int32) (*mq.ProducerResult, error) {
	kafkaMsg := &kafka.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: common.ConvertHeaderMap(m.Header),
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: partition,
		},
	}
	return p.produce(ctx, kafkaMsg)
}

func (p *Producer) Close() error {
	p.producer.Close()
	return nil
}

func (p *Producer) produce(ctx context.Context, msg *kafka.Message) (*mq.ProducerResult, error) {
	deliveryChan := make(chan kafka.Event)
	err := p.producer.Produce(msg, deliveryChan)
	if err != nil {
		kafkaErr, ok := err.(kafka.Error)
		if ok && kafkaErr.Code() == kafka.ErrState {
			return nil, mqerr.ErrProducerIsClosed
		}
		return nil, err
	}
	// 等待返回的报错
	e := <-deliveryChan
	if msg, ok := e.(*kafka.Message); ok {
		if msg.TopicPartition.Error != nil {
			return nil, msg.TopicPartition.Error
		}
	}

	return &mq.ProducerResult{}, nil
}
