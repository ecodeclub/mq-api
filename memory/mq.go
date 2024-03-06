package memory

import (
	"context"
	"fmt"
	"github.com/ecodeclub/mq-api/internal/pkg/validator"
	"log"
	"sync"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/mqerr"
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
		return fmt.Errorf("%w: %s", mqerr.ErrInvalidTopic, topic)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return mqerr.ErrMQIsClosed
	}
	_, ok := m.topics.Load(topic)
	if ok {
		return mqerr.ErrInvalidTopic
	}
	if partitions <= 0 {
		return mqerr.ErrInvalidPartition
	}
	m.topics.Store(topic, NewTopic(topic, partitions))
	return nil
}

func (m *MQ) Producer(topic string) (mq.Producer, error) {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return nil, mqerr.ErrMQIsClosed
	}
	t, ok := m.topics.Load(topic)
	if !ok {
		return nil, mqerr.ErrUnknownTopic
	}
	p := &Producer{
		locker: sync.RWMutex{},
		t:      t,
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
		return nil, mqerr.ErrMQIsClosed
	}
	t, ok := m.topics.Load(topic)
	if !ok {
		return nil, mqerr.ErrUnknownTopic
	}
	group, ok := t.consumerGroups.Load(groupID)
	if !ok {
		group = &ConsumerGroup{
			name:                      groupID,
			consumers:                 syncx.Map[string, *ConsumerMetaData]{},
			consumerPartitionBalancer: t.consumerPartitionBalancer,
			partitions:                t.partitions,
			balanceCh:                 make(chan struct{}, 10),
			status:                    StatusStable,
		}
		// 初始化分区消费进度
		partitionRecords := syncx.Map[int, PartitionRecord]{}
		for idx, _ := range t.partitions {
			partitionRecords.Store(idx, PartitionRecord{
				Index:  idx,
				Cursor: 0,
			})
		}
		group.partitionRecords = partitionRecords
	}
	consumer := group.JoinGroup()
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
		return mqerr.ErrMQIsClosed
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, t := range topics {
		topic, ok := m.topics.Load(t)
		if ok {
			topic.Close()
			m.topics.Delete(t)
		}

	}
	return nil
}
