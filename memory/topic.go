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
	"log"
	"sync"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/memory/consumerpartitionassigner/equaldivide"
	"github.com/ecodeclub/mq-api/memory/produceridgetter/hash"
	"github.com/ecodeclub/mq-api/mqerr"
)

type Topic struct {
	// 用[]*mq.Message表示一个分区
	locker     sync.RWMutex
	closed     bool
	name       string
	partitions []*Partition
	producers  []mq.Producer
	// 消费组
	consumerGroups syncx.Map[string, *ConsumerGroup]
	// 生产消息的时候获取分区号
	partitionIDGetter         PartitionIDGetter
	consumerPartitionBalancer ConsumerPartitionAssigner
}
type TopicOption func(t *Topic)

func NewTopic(name string, partitions int) *Topic {
	t := &Topic{
		name:                      name,
		consumerGroups:            syncx.Map[string, *ConsumerGroup]{},
		consumerPartitionBalancer: equaldivide.NewBalancer(),
		partitionIDGetter:         &hash.Getter{Partition: partitions},
	}
	partitionList := make([]*Partition, 0, partitions)
	for i := 0; i < partitions; i++ {
		partitionList = append(partitionList, NewPartition())
	}
	t.partitions = partitionList
	return t
}

func (t *Topic) addProducer(producer mq.Producer) error {
	t.locker.Lock()
	defer t.locker.Unlock()
	if t.closed {
		return mqerr.ErrMQIsClosed
	}
	t.producers = append(t.producers, producer)
	return nil
}

// addMessage 往分区里面添加消息
func (t *Topic) addMessage(msg *mq.Message, partition ...int64) error {
	var partitionID int64
	partitionLen := len(partition)
	switch partitionLen {
	case 0:
		partitionID = t.partitionIDGetter.GetPartitionID(string(msg.Key))
	case 1:
		partitionID = partition[0]
	default:
		return mqerr.ErrInvalidPartition
	}
	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return mqerr.ErrInvalidPartition
	}
	msg.Topic = t.name
	msg.Partition = partitionID
	t.partitions[partitionID].sendMsg(msg)
	log.Printf("生产消息 %s,消息为 %s", t.name, msg.Value)
	return nil
}

func (t *Topic) Close() error {
	t.locker.Lock()
	defer t.locker.Unlock()
	if !t.closed {
		t.consumerGroups.Range(func(key string, value *ConsumerGroup) bool {
			value.Close()
			return true
		})
		for _, producer := range t.producers {
			_ = producer.Close()
		}
	}
	return nil
}
