package memory

import (
	"context"

	"github.com/ecodeclub/mq-api"
)

type Producer struct {
	ProducerCh chan *mq.Message
	topic      string
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	m.Topic = p.topic
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p.ProducerCh <- m:
		return &mq.ProducerResult{}, nil
	}
}

func (p *Producer) ProduceWithPartition(ctx context.Context, _ *mq.Message, _ int) (*mq.ProducerResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return &mq.ProducerResult{}, nil
}

func (p *Producer) Close() error {
	return nil
}
