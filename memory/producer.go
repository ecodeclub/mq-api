// Copyright 2021 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"context"
	"github.com/ecodeclub/mq-api/internal/errs"
	"sync/atomic"

	"github.com/ecodeclub/mq-api"
)

type Producer struct {
	t      *Topic
	closed int32
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if p.isClosed() {
		return nil, errs.ErrProducerIsClosed
	}
	err := p.t.addMessage(m)
	return &mq.ProducerResult{}, err
}

func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partition int) (*mq.ProducerResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if p.isClosed() {
		return nil, errs.ErrProducerIsClosed
	}
	err := p.t.addMessageWithPartition(m, int64(partition))
	return &mq.ProducerResult{}, err
}

func (p *Producer) Close() error {
	atomic.StoreInt32(&p.closed, 1)
	return nil
}

func (p *Producer) isClosed() bool {
	return atomic.LoadInt32(&p.closed) == 1
}
