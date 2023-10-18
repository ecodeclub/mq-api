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
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"sync"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka/common"
	"github.com/ecodeclub/mq-api/mqerr"
	"github.com/segmentio/kafka-go"
	"io"
)

type Producer struct {
	topic          string
	producer       *kafka.Writer
	closed         bool
	address        string
	partitionConns map[int32]*kafka.Conn
	locker         sync.RWMutex
}

func (p *Producer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	p.locker.Lock()
	defer p.locker.Unlock()
	if p.closed {
		return nil, errors.Wrap(mqerr.ErrProducerIsClosed, "kafka: ")
	}
	kafkaMsg := kafka.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: common.ConvertToKafkaHeader(m.Header),
	}
	return &mq.ProducerResult{}, p.producer.WriteMessages(ctx, kafkaMsg)
}

func (p *Producer) ProduceWithPartition(ctx context.Context, m *mq.Message, partitionId int32) (*mq.ProducerResult, error) {
	p.locker.Lock()
	defer p.locker.Unlock()
	if p.closed {
		return nil, errors.Wrap(mqerr.ErrProducerIsClosed, "kafka: ")
	}
	conn, err := p.getPartitionConn(ctx, partitionId)
	if err != nil {
		return nil, err
	}
	_, err = conn.WriteMessages(kafka.Message{
		Value:   m.Value,
		Key:     m.Key,
		Headers: common.ConvertToKafkaHeader(m.Header),
	})
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.Wrap(mqerr.ErrProducerIsClosed, "kafka: ")
		}
		return nil, err
	}
	return &mq.ProducerResult{}, err
}

func (p *Producer) Close() error {
	p.locker.Lock()
	defer p.locker.Unlock()
	p.closed = true
	errList := make([]error, 0, len(p.partitionConns)+1)
	for _, conn := range p.partitionConns {
		err := conn.Close()
		if err != nil {
			errList = append(errList, err)
		}
	}
	err := p.producer.Close()
	if err != nil {
		errList = append(errList, err)
	}

	return multierr.Combine(errList...)
}

func (p *Producer) getPartitionConn(ctx context.Context, partitionId int32) (*kafka.Conn, error) {
	conn, ok := p.partitionConns[partitionId]
	if ok {
		return conn, nil
	}
	conn, err := kafka.DialLeader(ctx, "tcp", p.address, p.topic, int(partitionId))
	if err != nil {
		return nil, err
	}
	p.partitionConns[partitionId] = conn
	return conn, nil
}
