package kafka

import (
	"context"
	"errors"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka/common"
	"github.com/ecodeclub/mq-api/mqerr"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"sync"
	"time"
)

const (
	ReadTimeout = 100 * time.Millisecond
)

type Consumer struct {
	topic string
	id    string
	// 负责关闭开启的goroutine
	closeCh  chan struct{}
	closed   bool
	consumer *kafka.Reader
	once     sync.Once
	locker   sync.RWMutex
	msgCh    chan *mq.Message
}

func (c *Consumer) Consume(ctx context.Context) (*mq.Message, error) {
	if c.isClosed() {
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
	if c.isClosed() {
		return nil, mqerr.ErrConsumerIsClosed
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.msgCh, nil
}

func (c *Consumer) Close() error {
	var err error
	c.once.Do(func() {
		err = c.consumer.Close()
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
		close(c.msgCh)
	}()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), ReadTimeout)
		m, err := c.consumer.ReadMessage(ctx)
		cancel()
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				continue
			case errors.Is(err, io.EOF):
				return
			default:
				log.Printf("读取消息失败: %v", err)
				continue
			}
		}
		msg := common.ConvertToMqMsg(m)
		select {
		case c.msgCh <- msg:
		case <-c.closeCh:
			return
		}
	}

}

func (c *Consumer) isClosed() bool {
	c.locker.RLock()
	defer c.locker.RUnlock()
	return c.closed
}
