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
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/ecodeclub/mq-api/internal/errs"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/internal/pkg/validator"
	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/multierr"
)

// 默认分区副本数
const defaultReplicationFactor = 1

type MQ struct {
	address           []string
	controllerConn    *kafkago.Conn
	replicationFactor int

	locker   sync.RWMutex
	closed   bool
	closeErr error

	producers []mq.Producer
	consumers []mq.Consumer
}

func NewMQ(network string, address []string) (mq.MQ, error) {
	conn, err := kafkago.Dial(network, address[0])
	if err != nil {
		return nil, err
	}
	// 获取Kafka集群的控制器
	controller, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	// 与控制器建立连接
	var controllerConn *kafkago.Conn
	controllerConn, err = kafkago.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, err
	}
	return &MQ{
		address:           address,
		controllerConn:    controllerConn,
		replicationFactor: defaultReplicationFactor,
	}, nil
}

func (m *MQ) CreateTopic(ctx context.Context, name string, partitions int) error {
	if !validator.IsValidTopic(name) {
		return fmt.Errorf("%w: %s", errs.ErrInvalidTopic, name)
	}

	if partitions <= 0 {
		return fmt.Errorf("%w: %d", errs.ErrInvalidPartition, partitions)
	}

	m.locker.Lock()
	defer m.locker.Unlock()

	if m.closed {
		return fmt.Errorf("kafka: %w", errs.ErrMQIsClosed)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	cfg := kafkago.TopicConfig{Topic: name, NumPartitions: partitions, ReplicationFactor: m.replicationFactor}
	return m.controllerConn.CreateTopics(cfg)
}

// DeleteTopics 删除topic
func (m *MQ) DeleteTopics(ctx context.Context, topics ...string) error {
	m.locker.Lock()
	defer m.locker.Unlock()

	if m.closed {
		return fmt.Errorf("kafka: %w", errs.ErrMQIsClosed)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	err := m.controllerConn.DeleteTopics(topics...)
	var val kafkago.Error
	if errors.As(err, &val) && val == kafkago.UnknownTopicOrPartition {
		return nil
	}
	return err
}

func (m *MQ) Producer(topic string) (mq.Producer, error) {
	m.locker.Lock()
	defer m.locker.Unlock()

	if m.closed {
		return nil, fmt.Errorf("kafka: %w", errs.ErrMQIsClosed)
	}

	balancer, _ := NewSpecifiedPartitionBalancer(&kafkago.Hash{})
	p := NewProducer(m.address, topic, balancer)
	m.producers = append(m.producers, p)
	return p, nil
}

func (m *MQ) Consumer(topic, groupID string) (mq.Consumer, error) {
	m.locker.Lock()
	defer m.locker.Unlock()

	if m.closed {
		return nil, fmt.Errorf("kafka: %w", errs.ErrMQIsClosed)
	}

	c := NewConsumer(m.address, topic, groupID)
	m.consumers = append(m.consumers, c)

	go c.getMsgFromKafka()
	return c, nil
}

func (m *MQ) Close() error {
	m.locker.Lock()
	defer m.locker.Unlock()

	if !m.closed {
		errorList := make([]error, 0, len(m.producers)+len(m.consumers))
		for _, p := range m.producers {
			errorList = append(errorList, p.Close())
		}
		for _, c := range m.consumers {
			errorList = append(errorList, c.Close())
		}
		errorList = append(errorList, m.controllerConn.Close())
		m.closeErr = multierr.Combine(errorList...)

		m.closed = true
	}

	return m.closeErr
}
