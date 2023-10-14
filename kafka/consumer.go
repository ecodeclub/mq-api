package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/common"
	"log"
	"sync"
	"time"
)

type CustomConsumer struct {
	topic string
	id    string
	// 负责关闭开启的goroutine
	closeCh  chan struct{}
	consumer *kafka.Consumer
	once     sync.Once
	msgCh    chan *mq.Message
}

func (c *CustomConsumer) Consume(ctx context.Context) (*mq.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-c.msgCh:
		return m, nil
	}
}

func (c *CustomConsumer) ConsumeMsgCh(ctx context.Context) (<-chan *mq.Message, error) {
	return c.msgCh, nil
}

func (c *CustomConsumer) Close() error {
	var err error
	c.once.Do(func() {
		close(c.closeCh)
		//err = c.consumer.Close()
	})
	return err
}
func (c *CustomConsumer) ConsumerSeek(partition int64, offset int64) error {
	tp := kafka.TopicPartition{
		Topic:     &c.topic,
		Partition: int32(partition),
		Offset:    kafka.Offset(offset),
	}
	return c.consumer.Assign([]kafka.TopicPartition{tp})
}

// GetMsgFromKafka 完成持续从kafka内获取数据
func (c *CustomConsumer) GetMsgFromKafka() {
	defer c.consumer.Close()
	for {
		select {
		case <-c.closeCh:
			close(c.msgCh)
			return
		default:
			if c.consumer.IsClosed() {
				return
			}
			m, err := c.consumer.ReadMessage(2 * time.Second)
			if err == nil {
				msg := &mq.Message{
					Value:     m.Value,
					Key:       m.Key,
					Header:    common.ConvertHeaderSliceToMap(m.Headers),
					Topic:     *m.TopicPartition.Topic,
					Partition: int64(m.TopicPartition.Partition),
					Offset:    int64(m.TopicPartition.Offset),
				}
				select {
				case c.msgCh <- msg:
				case <-c.closeCh:
					close(c.msgCh)
					return
				}
			} else if !err.(kafka.Error).IsTimeout() {
				log.Println(fmt.Errorf("消息接受失败 %v", err))
			}
		}
	}
}
