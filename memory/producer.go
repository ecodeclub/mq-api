package memory

import (
	"context"
	"github.com/ecodeclub/mq-api"
)

type TopicProducer struct {
	ProducerCh chan *mq.Message
	topic      string
}

func (t *TopicProducer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	m.Topic = t.topic
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case t.ProducerCh <- m:
		return &mq.ProducerResult{}, nil
	}
}
