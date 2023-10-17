package kafka

import (
	"context"
	"github.com/pkg/errors"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka/common"
	"github.com/ecodeclub/mq-api/mqerr"
	"github.com/segmentio/kafka-go"
	"io"
)

type Producer struct {
	topic    string
	producer *kafka.Writer
	//closed   bool
	//locker   sync.RWMutex
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	kafkaMsg := kafka.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: common.ConvertToKafkaHeader(m.Header),
		Topic:   p.topic,
	}
	return p.produce(ctx, kafkaMsg)
}

func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partition int32) (*mq.ProducerResult, error) {
	kafkaMsg := kafka.Message{
		Value:     m.Value,
		Key:       m.Key,
		Headers:   common.ConvertToKafkaHeader(m.Header),
		Topic:     p.topic,
		Partition: int(partition),
	}
	return p.produce(ctx, kafkaMsg)
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func (p *Producer) produce(ctx context.Context, msg kafka.Message) (*mq.ProducerResult, error) {
	err := p.producer.WriteMessages(ctx, msg)
	if errors.Is(err, io.EOF) {
		return nil, errors.Wrap(mqerr.ErrMQIsClosed, "kafka: ")
	}
	return &mq.ProducerResult{}, err
}
