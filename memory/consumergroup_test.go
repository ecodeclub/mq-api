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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api/memory/consumerpartitionassigner/equaldivide"
	"github.com/stretchr/testify/assert"
)

// 测试场景： 不断有 消费者加入 消费组，最后达成的效果，调用consumerGroup的close方法成功之后，consumerGroup里面没有consumer存在且所有的consumer都是关闭的状态

func TestConsumerGroup_Close(t *testing.T) {
	t.Parallel()
	cg := &ConsumerGroup{
		name:                      "test_group",
		consumers:                 syncx.Map[string, *Consumer]{},
		consumerPartitionAssigner: equaldivide.NewAssigner(),
		partitions: []*Partition{
			NewPartition(),
			NewPartition(),
			NewPartition(),
		},
		balanceCh: make(chan struct{}, defaultBalanceChLen),
		status:    StatusStable,
	}
	partitionRecords := syncx.Map[int, PartitionRecord]{}
	for idx := range cg.partitions {
		partitionRecords.Store(idx, PartitionRecord{
			Index:  idx,
			Offset: 0,
		})
	}
	cg.partitionRecords = &partitionRecords
	var wg sync.WaitGroup
	mu := &sync.RWMutex{}
	consumerGroups := make([]*Consumer, 0, 100)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := cg.JoinGroup()
			if err != nil {
				assert.Equal(t, ErrConsumerGroupClosed, err)
				return
			}
			mu.Lock()
			consumerGroups = append(consumerGroups, c)
			mu.Unlock()
		}()
	}
	time.Sleep(100 * time.Millisecond)
	cg.Close()
	wg.Wait()
	// consumerGroup中没有消费者
	var flag atomic.Bool
	cg.consumers.Range(func(key string, value *Consumer) bool {
		flag.Store(true)
		return true
	})
	assert.False(t, flag.Load())
	// 所有加入的消费者都是关闭状态
	cg.consumers.Range(func(key string, value *Consumer) bool {
		assert.True(t, value.closed)
		return true
	})
}
