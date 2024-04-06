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

package kafka

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ecodeclub/mq-api/internal/errs"

	"github.com/ecodeclub/ekit/retry"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka/common"
	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
)

type Producer struct {
	topic  string
	writer *kafkago.Writer
	locker *sync.RWMutex

	closeOnce *sync.Once
	closed    bool
	closeErr  error
}

func NewProducer(address []string, topic string, balancer kafkago.Balancer) *Producer {
	return &Producer{
		topic:  topic,
		locker: &sync.RWMutex{},
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(address...),
			Topic:    topic,
			Balancer: balancer,
		},
		closed:    false,
		closeOnce: &sync.Once{},
	}
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	return p.produce(ctx, m, nil)
}

// ProduceWithPartition 并没有校验 partition 的正确性。
func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partition int) (*mq.ProducerResult, error) {
	return p.produce(ctx, m, metaMessage{SpecifiedPartitionKey: partition})
}

func (p *Producer) produce(ctx context.Context, m *mq.Message, meta metaMessage) (*mq.ProducerResult, error) {
	message := p.newKafkaMessage(m, meta)

	const (
		initialInterval = 100 * time.Millisecond
		maxInterval     = 10 * time.Second
		maxRetries      = 50
	)

	strategy, _ := retry.NewExponentialBackoffRetryStrategy(initialInterval, maxInterval, maxRetries)

	for {
		err := p.writer.WriteMessages(ctx, message)
		if err == nil {
			return &mq.ProducerResult{}, nil
		}
		if errors.Is(err, io.ErrClosedPipe) {
			return &mq.ProducerResult{}, fmt.Errorf("kafka: %w", errs.ErrProducerIsClosed)
		}
		// 控制流走到这Topic和Partition已经验证合法
		// 要么选主阶段、要么分区在broker间移动,因此这两种情况需要重试
		if errors.Is(err, kafkago.LeaderNotAvailable) || errors.Is(err, kafkago.UnknownTopicOrPartition) {
			duration, ok := strategy.Next()
			if ok {
				time.Sleep(duration)
				continue
			}
		}
		return &mq.ProducerResult{}, err
	}
}

func (p *Producer) newKafkaMessage(m *mq.Message, meta metaMessage) kafkago.Message {
	message := kafkago.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: common.ConvertToKafkaHeader(m.Header),
	}
	if meta != nil {
		message.WriterData = meta
	}
	return message
}

func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = p.writer.Close()
	})
	return p.closeErr
}
