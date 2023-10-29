package memory

import (
	"context"

	"github.com/ecodeclub/mq-api"
)

type Consumer struct {
	groupID     string
	messageChan chan *mq.Message
}

func (c *Consumer) Consume(ctx context.Context) (*mq.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-c.messageChan:
		return msg, nil
	}
}

func (c *Consumer) ConsumeChan(ctx context.Context) (<-chan *mq.Message, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.messageChan, nil
}

func (c *Consumer) Close() error {
	return nil
}
