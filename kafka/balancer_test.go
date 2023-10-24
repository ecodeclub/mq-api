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
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestSpecifiedPartitionBalancer(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string

		newBalancer func() (kafkago.Balancer, error)
		message     kafkago.Message
		partitions  []int

		wantPartition int
		wantErr       error
	}{
		{
			name: "创建失败_返回错误",
			newBalancer: func() (kafkago.Balancer, error) {
				return NewSpecifiedPartitionBalancer(nil)
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "创建成功_可以正常使用",
			newBalancer: func() (kafkago.Balancer, error) {
				return NewSpecifiedPartitionBalancer(&kafkago.RoundRobin{})
			},
			message:       kafkago.Message{Value: []byte("Hello")},
			partitions:    []int{0, 1},
			wantPartition: 0,
		},
		{
			name: "指定分区时_元数据中用于获取指定分区的key错误_降级为默认负载均衡器",
			newBalancer: func() (kafkago.Balancer, error) {
				return NewSpecifiedPartitionBalancer(&kafkago.RoundRobin{})
			},
			message:       kafkago.Message{Value: []byte("Hello"), WriterData: metaMessage{"wrongKey": 1}},
			partitions:    []int{0, 1},
			wantPartition: 0,
		},
		{
			name: "指定分区时_元数据中用于作为分区值的value错误_降级为默认负载均衡器",
			newBalancer: func() (kafkago.Balancer, error) {
				return NewSpecifiedPartitionBalancer(&kafkago.RoundRobin{})
			},
			message:       kafkago.Message{Value: []byte("Hello"), WriterData: metaMessage{SpecifiedPartitionKey: "1"}},
			partitions:    []int{0, 1},
			wantPartition: 0,
		},
		{
			name: "指定分区成功",
			newBalancer: func() (kafkago.Balancer, error) {
				return NewSpecifiedPartitionBalancer(&kafkago.RoundRobin{})
			},
			message:       kafkago.Message{Value: []byte("Hello"), WriterData: metaMessage{SpecifiedPartitionKey: 1}},
			partitions:    []int{0, 1},
			wantPartition: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			balancer, err := tc.newBalancer()
			assert.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			partition := balancer.Balance(tc.message, tc.partitions...)
			assert.Equal(t, tc.wantPartition, partition)
		})
	}
}
