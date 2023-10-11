package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ecodeclub/mq-api"
)

type CustomProducer struct {
	producer *kafka.Producer
}

// Produce method for CustomProducer
func (p *CustomProducer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	// Create a Kafka message
	kafkaMsg := &kafka.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: convertHeaderMap(m.Header),
		TopicPartition: kafka.TopicPartition{
			Topic: &m.Topic,
		},
	}

	deliveryChan := make(chan kafka.Event)
	err := p.producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
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

func convertHeaderMap(headerMap mq.Header) []kafka.Header {
	headers := make([]kafka.Header, 0, len(headerMap))
	for key, value := range headerMap {
		header := kafka.Header{
			Key:   key,
			Value: []byte(value),
		}
		headers = append(headers, header)
	}
	return headers
}
