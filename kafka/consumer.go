package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/common"
	"github.com/ecodeclub/mq-api/mqerr"
	"log"
	"sync"
	"time"
)

type Consumer struct {
	topic string
	id    string
	// 负责关闭开启的goroutine
	closeCh  chan struct{}
	closed   bool
	consumer *kafka.Consumer
	once     sync.Once
	locker   sync.RWMutex
	msgCh    chan *mq.Message
}

func (c *Consumer) Consume(ctx context.Context) (*mq.Message, error) {
	if c.closed {
		return nil, mqerr.ErrConsumerIsClosed
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-c.msgCh:
		return m, nil
	case <-c.closeCh:
		return nil, mqerr.ErrConsumerIsClosed
	}
}

func (c *Consumer) ConsumeChan(ctx context.Context) (<-chan *mq.Message, error) {
	if c.closed {
		return nil, mqerr.ErrConsumerIsClosed
	}
	return c.msgCh, nil
}

func (c *Consumer) Close() error {
	var err error
	c.once.Do(func() {
		close(c.closeCh)
	})
	c.locker.Lock()
	c.closed = true
	c.locker.Unlock()
	return err
}

// getMsgFromKafka 完成持续从kafka内获取数据
func (c *Consumer) getMsgFromKafka() {
	defer func() {
		c.consumer.Close()
		close(c.msgCh)
	}()
	for {
		select {
		case <-c.closeCh:
			return
		default:
			m, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err == nil {
				msg := &mq.Message{
					Value:       m.Value,
					Key:         m.Key,
					Header:      common.ConvertHeaderSliceToMap(m.Headers),
					Topic:       *m.TopicPartition.Topic,
					PartitionID: int64(m.TopicPartition.Partition),
					Offset:      int64(m.TopicPartition.Offset),
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

func (c *Consumer) isClosed() bool {
	c.locker.RLock()
	defer c.locker.RUnlock()
	return c.closed
}
