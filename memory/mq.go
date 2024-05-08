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
	"fmt"
	"log"
	"sync"

	"github.com/ecodeclub/mq-api/internal/pkg/validator"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/internal/errs"
)

const (
	defaultBalanceChLen = 10
	defaultPartitions   = 3
)

type MQ struct {
	locker sync.RWMutex
	closed bool
	topics syncx.Map[string, *Topic]
}

func NewMQ() mq.MQ {
	return &MQ{
		topics: syncx.Map[string, *Topic]{},
	}
}

func (m *MQ) CreateTopic(ctx context.Context, topic string, partitions int) error {
	if !validator.IsValidTopic(topic) {
		return fmt.Errorf("%w: %s", errs.ErrInvalidTopic, topic)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return errs.ErrMQIsClosed
	}
	if partitions <= 0 {
		return errs.ErrInvalidPartition
	}
	_, ok := m.topics.Load(topic)
	if !ok {
		m.topics.Store(topic, newTopic(topic, partitions))
	}
	return nil
}

func (m *MQ) Producer(topic string) (mq.Producer, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return nil, errs.ErrMQIsClosed
	}
	t, ok := m.topics.Load(topic)
	if !ok {
		t = newTopic(topic, defaultPartitions)
		m.topics.Store(topic, t)
	}
	p := &Producer{
		t: t,
	}
	err := t.addProducer(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (m *MQ) Consumer(topic, groupID string) (mq.Consumer, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return nil, errs.ErrMQIsClosed
	}
	t, ok := m.topics.Load(topic)
	if !ok {
		t = newTopic(topic, defaultPartitions)
		m.topics.Store(topic, t)
	}
	group, ok := t.consumerGroups.Load(groupID)
	if !ok {
		group = &ConsumerGroup{
			name:                      groupID,
			consumers:                 syncx.Map[string, *Consumer]{},
			consumerPartitionAssigner: t.consumerPartitionAssigner,
			partitions:                t.partitions,
			balanceCh:                 make(chan struct{}, defaultBalanceChLen),
			status:                    StatusStable,
		}
		// 初始化分区消费进度
		partitionRecords := syncx.Map[int, PartitionRecord]{}
		for idx := range t.partitions {
			partitionRecords.Store(idx, PartitionRecord{
				Index:  idx,
				Offset: 0,
			})
		}
		group.partitionRecords = &partitionRecords
	}
	consumer, err := group.JoinGroup()
	if err != nil {
		return nil, err
	}
	t.consumerGroups.Store(groupID, group)
	return consumer, nil
}

func (m *MQ) Close() error {
	m.locker.Lock()
	defer m.locker.Unlock()
	m.closed = true
	m.topics.Range(func(key string, value *Topic) bool {
		err := value.Close()
		if err != nil {
			log.Printf("topic: %s关闭失败 %v", key, err)
		}
		return true
	})

	return nil
}

func (m *MQ) DeleteTopics(ctx context.Context, topics ...string) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return errs.ErrMQIsClosed
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, t := range topics {
		topic, ok := m.topics.Load(t)
		if ok {
			err := topic.Close()
			if err != nil {
				log.Printf("topic: %s关闭失败 %v", t, err)
				continue
			}
			m.topics.Delete(t)
		}

	}
	return nil
}
