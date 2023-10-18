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
	"net"
	"strconv"
	"sync"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/mqerr"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/multierr"
)

// consumerChannel先默认1000
const msgChannelSize = 1000

// 默认分区副本数
const defaultReplicationFactor = 1

// 默认分区连接数
const defaultPartitionConn = 16

type MQ struct {
	// 用于创建topic
	address           []string
	conn              *kafka.Conn
	controllerConn    *kafka.Conn
	locker            sync.RWMutex
	closed            bool
	replicationFactor int
	// 方便释放资源
	producers []mq.Producer
	consumers []mq.Consumer
}

func NewMQ(network string, address []string) (mq.MQ, error) {
	conn, err := kafka.Dial(network, address[0])
	if err != nil {
		return nil, err
	}
	// 获取Kafka集群的控制器
	controller, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	// 与控制器建立连接
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, err
	}
	return &MQ{
		address:           address,
		conn:              conn,
		controllerConn:    controllerConn,
		replicationFactor: defaultReplicationFactor,
	}, nil
}

// DeleteTopics 删除topic
func (m *MQ) DeleteTopics(ctx context.Context, topics []string) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return errors.Wrap(mqerr.ErrMQIsClosed, "kafka: ")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err := m.controllerConn.DeleteTopics(topics...)
	var val kafka.Error
	if errors.As(err, &val) && val == kafka.UnknownTopicOrPartition {
		return nil
	}
	return err
}

func (m *MQ) Topic(ctx context.Context, name string, partitions int) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return errors.Wrap(mqerr.ErrMQIsClosed, "kafka: ")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: m.replicationFactor,
		},
	}
	return m.controllerConn.CreateTopics(topicConfigs...)
}

func (m *MQ) Producer(topic string) (mq.Producer, error) {
	if m.isClosed() {
		return nil, mqerr.ErrMQIsClosed
	}
	w := &kafka.Writer{
		Addr:     kafka.TCP(m.address...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
	p := &Producer{
		topic:          topic,
		producer:       w,
		partitionConns: make(map[int32]*kafka.Conn, defaultPartitionConn),
	}
	m.locker.Lock()
	m.producers = append(m.producers, p)
	m.locker.Unlock()
	return p, nil
}

func (m *MQ) Consumer(topic, groupID string) (mq.Consumer, error) {
	if m.isClosed() {
		return nil, errors.Wrap(mqerr.ErrMQIsClosed, "kafka: ")
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: m.address,
		Topic:   topic,
		GroupID: groupID,
	})
	closeCh := make(chan struct{})
	msgCh := make(chan *mq.Message, msgChannelSize)
	c := &Consumer{
		topic:    topic,
		id:       groupID,
		closeCh:  closeCh,
		msgCh:    msgCh,
		consumer: r,
	}
	m.locker.Lock()
	m.consumers = append(m.consumers, c)
	m.locker.Unlock()
	go c.getMsgFromKafka()

	return c, nil
}

func (m *MQ) Close() error {
	m.locker.Lock()
	defer m.locker.Unlock()
	errorList := make([]error, 0, len(m.consumers))
	for _, p := range m.producers {
		err := p.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	for _, c := range m.consumers {
		err := c.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	m.closed = true

	return multierr.Combine(errorList...)
}

func (m *MQ) isClosed() bool {
	m.locker.RLock()
	defer m.locker.RUnlock()
	return m.closed
}
