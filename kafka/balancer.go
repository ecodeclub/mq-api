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
	"fmt"

	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	SpecifiedPartitionKey = "specifiedPartition"
)

var ErrInvalidArgument = errors.New("invalid argument")

type metaMessage map[string]any

// SpecifiedPartitionBalancer 借助kafka客户端提供的Balancer接口实现向指定分区生产消息
// 如果在message.WriterData中找到指定的partition信息则直接用其作为目标partition
// 如果没有找到则使用默认负载均衡器计算目标partition
type SpecifiedPartitionBalancer struct {
	defaultBalancer kafkago.Balancer
}

func NewSpecifiedPartitionBalancer(defaultBalancer kafkago.Balancer) (*SpecifiedPartitionBalancer, error) {
	if defaultBalancer == nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, "default balancer should not be nil")
	}
	return &SpecifiedPartitionBalancer{defaultBalancer: defaultBalancer}, nil
}

func (b *SpecifiedPartitionBalancer) Balance(msg kafkago.Message, partitions ...int) (partition int) {
	meta, ok := msg.WriterData.(metaMessage)
	if !ok {
		return b.defaultBalancer.Balance(msg, partitions...)
	}
	v, ok := meta[SpecifiedPartitionKey]
	if !ok {
		return b.defaultBalancer.Balance(msg, partitions...)
	}
	p, ok := v.(int)
	if !ok {
		return b.defaultBalancer.Balance(msg, partitions...)
	}
	return p
}
