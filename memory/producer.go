package memory

import (
	"context"
	"sync"

	"github.com/ecodeclub/mq-api/mqerr"

	"github.com/ecodeclub/mq-api"
)

type Producer struct {
	t      *Topic
	closed bool
	locker sync.RWMutex
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// 将partition设为 -1，按系统分配算法分配到某个分区
	if p.isClosed() {
		return nil, mqerr.ErrProducerIsClosed
	}
	err := p.t.addMessage(m)
	return &mq.ProducerResult{}, err
}

func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partition int) (*mq.ProducerResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if p.isClosed() {
		return nil, mqerr.ErrProducerIsClosed
	}
	err := p.t.addMessage(m, int64(partition))
	return &mq.ProducerResult{}, err
}

func (p *Producer) Close() error {
	p.locker.Lock()
	defer p.locker.Unlock()
	p.closed = true
	return nil
}

func (p *Producer) isClosed() bool {
	p.locker.RLock()
	defer p.locker.RUnlock()
	return p.closed
}
