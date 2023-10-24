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

package mq

import "context"

// MQ 是消息队列的抽象用于创建Topic、生产者及消费者,MQ可以被多个协程并发访问
type MQ interface {
	// CreateTopic 用于创建topic并使用partitions指定分区数。分区从0开始编号,例如分区数为4，那么分区号就是0-3
	CreateTopic(ctx context.Context, topic string, partitions int) error
	// DeleteTopics 用于删除一个或多个topic
	DeleteTopics(ctx context.Context, topics ...string) error
	// Producer 用于创建某个topic的生产者
	Producer(topic string) (Producer, error)
	// Consumer 用于创建某个topic的消费者并使用groupID指定消费者所属消费组
	Consumer(topic string, groupID string) (Consumer, error)
	// Close 用于关闭消息队列,释放所有建立的Producer和Consumer资源，多次调用返回的error与第一次调用返回的error相同
	// 返回的error为由MQ抽象创建的Consumer和Producer的Close方法返回的error拼接而成
	Close() error
}

type Header map[string]string

// Message 是消息队列中传递的消息封装
type Message struct {
	// 消息本体，存储业务消息
	Value []byte
	// 对标kafka中的key，用于分区的。可以省略
	Key []byte
	// 对标kafka的header，用于传递一些自定义的元数据。
	Header Header
	// 消息主题
	Topic string
	// 分区ID
	Partition int64
	// 偏移量
	Offset int64
}

type ProducerResult struct{}

// Producer 是生产者抽象，用于向指定Topic发送/生产消息,可以被多个协程并发访问
type Producer interface {
	// Produce 不指定分区发送消息，发送消息时，消息所在分区不确定
	Produce(ctx context.Context, m *Message) (*ProducerResult, error)
	// ProduceWithPartition 指定分区发送消息
	ProduceWithPartition(ctx context.Context, m *Message, partition int) (*ProducerResult, error)
	// Close 用于释放资源，多次调用返回的error与第一次调用返回的error相同
	Close() error
}

// Consumer 是消费者的抽象，用于从指定Topic接收/消费消息,可以被多个协程并发访问
type Consumer interface {
	// Consume 获取单条信息
	Consume(ctx context.Context) (*Message, error)
	// ConsumeChan  从返回的channel中获取mq中的消息
	ConsumeChan(ctx context.Context) (<-chan *Message, error)
	// Close 用于释放资源，多次调用返回的error与第一次调用返回的error相同
	Close() error
}
