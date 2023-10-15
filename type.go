package mq

import "context"

type Header map[string]string

type Message struct {
	// 消息本体
	Value []byte
	Key   []byte
	// 对标kafka的header，用于传递一些自定义的元数据。
	Header Header
	Topic  string
	// 分区ID
	PartitionID int64
	// 偏移量
	Offset int64
}

type ProducerResult struct {
}

type Producer interface {
	// Produce 不指定分区发送消息，具体消息在哪个分区看具体实现
	Produce(ctx context.Context, m *Message) (*ProducerResult, error)
	// ProduceWithPartition 指定分区发送消息
	ProduceWithPartition(ctx context.Context, m *Message, partitionID int32) (*ProducerResult, error)
	// Close 释放连接资源，并关闭开启的goroutine
	Close() error
}

type Consumer interface {
	// Consume 获取单条信息
	Consume(ctx context.Context) (*Message, error)
	// ConsumeChan  从返回的channel中获取mq中的消息
	ConsumeChan(ctx context.Context) (<-chan *Message, error)
	// Close 释放连接资源，并关闭开启的goroutine
	Close() error
}

type MQ interface {
	// Topic 创建topic并制定topic的分区数，partitions 表示分区数。分区从0开始编号 例如分区数为4，那么分区号就是0-3
	Topic(ctx context.Context, topic string, partitions int) error
	// Producer 创建某个topic的生产者
	Producer(topic string) (Producer, error)
	// Consumer 创建某个topic的消费者,groupID消费组id
	Consumer(topic string, groupID string) (Consumer, error)
	// Close 释放所有建立的Producer和Consumer资源
	Close() error
	// ClearTopic 清理topic,删除所有传进来的topic
	ClearTopic(ctx context.Context, topics []string) error
}
