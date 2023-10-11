package memory

import (
	"context"
	"github.com/ecodeclub/mq-api"
)

type TopicConsumer struct {
	cursor int64

	consumerCh chan *mq.Message
}

func (t *TopicConsumer) Consume(ctx context.Context) (*mq.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-t.consumerCh:
		return msg, nil
	}
}

func (t *TopicConsumer) ConsumeMsgCh(ctx context.Context) (<-chan *mq.Message, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return t.consumerCh, nil
}
