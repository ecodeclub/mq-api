package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ecodeclub/mq-api"
)

type CustomConsumer struct {
	topic    string
	consumer *kafka.Consumer
}

func (c *CustomConsumer) Consume(ctx context.Context) (*mq.Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			ev := c.consumer.Poll(100) // Poll for Kafka events
			switch e := ev.(type) {
			case *kafka.Message:
				message := &mq.Message{
					Value:     e.Value,
					Key:       e.Key,
					Header:    convertHeaderSliceToMap(e.Headers),
					Topic:     *e.TopicPartition.Topic,
					Partition: int64(e.TopicPartition.Partition),
					Offset:    int64(e.TopicPartition.Offset),
				}
				return message, nil
			case kafka.Error:
				return nil, e
			}
		}
	}
}

func (c *CustomConsumer) ConsumeMsgCh(ctx context.Context) (<-chan *mq.Message, error) {
	messageCh := make(chan *mq.Message)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(messageCh)
				return
			default:
				ev := c.consumer.Poll(100) // Poll for Kafka events
				switch e := ev.(type) {
				case *kafka.Message:
					message := &mq.Message{
						Value:     e.Value,
						Key:       e.Key,
						Header:    convertHeaderSliceToMap(e.Headers),
						Topic:     *e.TopicPartition.Topic,
						Partition: int64(e.TopicPartition.Partition),
						Offset:    int64(e.TopicPartition.Offset),
					}
					messageCh <- message
				case kafka.Error:
					messageCh <- &mq.Message{Value: []byte(e.String())}
				}
			}
		}
	}()

	return messageCh, nil
}

func (c *CustomConsumer) ConsumerSeek(partition int64, offset int64) error {
	tp := kafka.TopicPartition{
		Topic:     &c.topic,
		Partition: int32(partition),
		Offset:    kafka.Offset(offset),
	}
	return c.consumer.Assign([]kafka.TopicPartition{tp})
}

func convertHeaderSliceToMap(headers []kafka.Header) mq.Header {
	headerMap := mq.Header{}
	for _, header := range headers {
		key := header.Key
		value := string(header.Value)
		headerMap[key] = value
	}

	return headerMap
}
