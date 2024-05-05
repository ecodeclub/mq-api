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
	"strconv"
	"sync"
	"testing"

	"github.com/ecodeclub/mq-api"
	"github.com/stretchr/testify/assert"
)

func Test_Partition(t *testing.T) {
	t.Parallel()
	p := NewPartition()
	for i := 0; i < 5; i++ {
		msg := &mq.Message{Value: []byte(strconv.Itoa(i))}
		p.append(msg)
	}
	msgs := p.getBatch(2, 2)
	assert.Equal(t, []*mq.Message{
		{
			Value:  []byte(strconv.Itoa(2)),
			Offset: 2,
		},
		{
			Value:  []byte(strconv.Itoa(3)),
			Offset: 3,
		},
	}, msgs)
	msgs = p.getBatch(2, 5)
	assert.Equal(t, []*mq.Message{
		{
			Value:  []byte(strconv.Itoa(2)),
			Offset: 2,
		},
		{
			Value:  []byte(strconv.Itoa(3)),
			Offset: 3,
		},
		{
			Value:  []byte(strconv.Itoa(4)),
			Offset: 4,
		},
	}, msgs)
}

// 测试多个goroutine往同一个队列中发送消息
func Test_PartitionConcurrent(t *testing.T) {
	t.Parallel()
	// 测试多个goroutine往partition里写
	p2 := NewPartition()
	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		index := i * 5
		go func() {
			defer wg.Done()
			for j := index; j < index+5; j++ {
				p2.append(&mq.Message{
					Value: []byte(strconv.Itoa(j)),
				})
			}
		}()
	}
	wg.Wait()
	msgs := p2.getBatch(0, 16)
	for idx := range msgs {
		msgs[idx].Partition = 0
		msgs[idx].Offset = 0
	}
	wantVal := []*mq.Message{
		{
			Value: []byte("0"),
		},
		{
			Value: []byte("1"),
		},
		{
			Value: []byte("2"),
		},
		{
			Value: []byte("3"),
		},
		{
			Value: []byte("4"),
		},
		{
			Value: []byte("5"),
		},
		{
			Value: []byte("6"),
		},
		{
			Value: []byte("7"),
		},
		{
			Value: []byte("8"),
		},
		{
			Value: []byte("9"),
		},
		{
			Value: []byte("10"),
		},
		{
			Value: []byte("11"),
		},
		{
			Value: []byte("12"),
		},
		{
			Value: []byte("13"),
		},
		{
			Value: []byte("14"),
		},
	}
	assert.ElementsMatch(t, wantVal, msgs)
}
