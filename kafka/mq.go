package kafka

import (
	"context"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/mqerr"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/multierr"
	"net"
	"strconv"
	"sync"
)

// 先默认1000
const msgChannelSize = 1000
const defaultReplicationFactor = 1

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
		address:        address,
		conn:           conn,
		controllerConn: controllerConn,
	}, nil
}

// ClearTopic 删除topic
func (m *MQ) ClearTopic(ctx context.Context, topics []string) error {
	m.locker.Lock()
	defer m.locker.Unlock()
	if m.closed {
		return errors.Wrap(mqerr.ErrMQIsClosed, "kafka: ")
	}
	err := m.controllerConn.DeleteTopics(topics...)
	if err.(kafka.Error) == kafka.UnknownTopicOrPartition {
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
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: defaultReplicationFactor,
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
		Balancer: &kafka.LeastBytes{},
	}
	p := &Producer{
		topic:    topic,
		producer: w,
	}
	m.locker.Lock()
	m.producers = append(m.producers, p)
	m.locker.Unlock()
	return p, nil
}

func (m *MQ) Consumer(topic string, groupID string) (mq.Consumer, error) {
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
