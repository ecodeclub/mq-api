package e2e

import (
	"context"
	"github.com/ecodeclub/mq-api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

type BaseSuite struct {
	suite.Suite
	mqCreator MqCreator
}

type MqCreator interface {
	InitMq() mq.MQ
}
type ProducerMsg struct {
	partition int32
	msg       *mq.Message
}

// 测试消费组
func (b *BaseSuite) TestMSMQConsumer_DiffGroup() {
	testcases := []struct {
		name         string
		topic        string
		partitions   int64
		input        []*mq.Message
		consumers    func(mqm mq.MQ) []mq.Consumer
		consumerFunc func(c mq.Consumer) []*mq.Message
		wantVal      []*mq.Message
	}{
		{
			name:       "多个消费组,多个消费组并行消费",
			topic:      "test_topic1",
			partitions: 4,
			input: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic1", "c1")
				require.NoError(b.T(), err)
				c12, err := mqm.Consumer("test_topic1", "c1")
				require.NoError(b.T(), err)
				c13, err := mqm.Consumer("test_topic1", "c1")
				require.NoError(b.T(), err)
				c21, err := mqm.Consumer("test_topic1", "c2")
				require.NoError(b.T(), err)
				c22, err := mqm.Consumer("test_topic1", "c2")
				require.NoError(b.T(), err)
				c23, err := mqm.Consumer("test_topic1", "c2")
				require.NoError(b.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
					c21,
					c22,
					c23,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(b.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic1",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic1",
				},
			},
		},
		{
			name:       "一个消费组竞争消费",
			topic:      "test_topic2",
			partitions: 4,
			input: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic2", "c1")
				require.NoError(b.T(), err)
				c12, err := mqm.Consumer("test_topic2", "c1")
				require.NoError(b.T(), err)
				c13, err := mqm.Consumer("test_topic2", "c1")
				require.NoError(b.T(), err)
				c14, err := mqm.Consumer("test_topic2", "c1")
				require.NoError(b.T(), err)
				c15, err := mqm.Consumer("test_topic2", "c1")
				require.NoError(b.T(), err)
				c16, err := mqm.Consumer("test_topic2", "c1")
				require.NoError(b.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
					c14,
					c15,
					c16,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(b.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic2",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic2",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic2",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic2",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic2",
				},
			},
		},
	}
	for _, tc := range testcases {
		b.T().Run(tc.name, func(t *testing.T) {
			mqm := b.mqCreator.InitMq()
			err := mqm.Topic(context.Background(), tc.topic, int(tc.partitions))
			require.NoError(t, err)
			p, err := mqm.Producer(tc.topic)
			require.NoError(t, err)
			consumers := tc.consumers(mqm)
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			var wg sync.WaitGroup
			locker := sync.RWMutex{}
			for _, c := range consumers {
				newc := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					msgs := tc.consumerFunc(newc)
					locker.Lock()
					ans = append(ans, msgs...)
					locker.Unlock()
				}()
			}
			for _, msg := range tc.input {
				_, err := p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			time.Sleep(5 * time.Second)
			err = mqm.Close()
			require.NoError(t, err)
			wg.Wait()
			assert.ElementsMatch(t, tc.wantVal, genMsg(ans))
		})
	}
}

// 测试同一partition下的顺序
func (b *BaseSuite) TestMSMQConsumer_PartitionSort() {
	testcases := []struct {
		name         string
		topic        string
		partitions   int64
		input        []*mq.Message
		consumers    func(mqm mq.MQ) []mq.Consumer
		consumerFunc func(c mq.Consumer) []*mq.Message
		wantVal      []*mq.Message
	}{
		{
			name:       "消息有序",
			topic:      "test_topic3",
			partitions: 3,
			input: []*mq.Message{
				{
					Key:   []byte("1"),
					Value: []byte("1"),
				},
				{
					Key:   []byte("1"),
					Value: []byte("2"),
				},
				{
					Key:   []byte("1"),
					Value: []byte("3"),
				},
				{
					Key:   []byte("1"),
					Value: []byte("4"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("1"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("2"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("3"),
				},
				{
					Key:   []byte("4"),
					Value: []byte("4"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic3", "c1")
				require.NoError(b.T(), err)
				c12, err := mqm.Consumer("test_topic3", "c1")
				require.NoError(b.T(), err)
				c13, err := mqm.Consumer("test_topic3", "c1")
				require.NoError(b.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(b.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("2"),
					Key:   []byte("1"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("3"),
					Key:   []byte("1"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("4"),
					Key:   []byte("1"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("1"),
					Key:   []byte("4"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("2"),
					Key:   []byte("4"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("3"),
					Key:   []byte("4"),
					Topic: "test_topic3",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic3",
				},
			},
		},
	}
	for _, tc := range testcases {
		b.T().Run(tc.name, func(t *testing.T) {
			mqm := b.mqCreator.InitMq()
			err := mqm.Topic(context.Background(), tc.topic, int(tc.partitions))
			require.NoError(t, err)
			p, err := mqm.Producer(tc.topic)
			require.NoError(t, err)
			consumers := tc.consumers(mqm)
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			var wg sync.WaitGroup
			locker := sync.RWMutex{}
			for _, c := range consumers {
				newc := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					msgs := tc.consumerFunc(newc)
					locker.Lock()
					ans = append(ans, msgs...)
					locker.Unlock()
				}()
			}
			for _, msg := range tc.input {
				_, err := p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			time.Sleep(5 * time.Second)
			err = mqm.Close()
			require.NoError(t, err)
			wg.Wait()
			wantMap := getMsgMap(tc.wantVal)
			actualMap := getMsgMap(genMsg(ans))
			assert.Equal(t, wantMap, actualMap)
		})
	}
}

// 测试发送到指定分区
func (b *BaseSuite) TestMSMQProducer_ProduceWithPartition() {
	testcases := []struct {
		name         string
		topic        string
		partitions   int64
		input        []ProducerMsg
		consumerFunc func(c mq.Consumer) []*mq.Message
		wantVal      []*mq.Message
	}{
		{
			name:       "生产消息到指定分区",
			topic:      "test_topic4",
			partitions: 4,
			input: []ProducerMsg{
				{
					partition: 0,
					msg: &mq.Message{
						Key:   []byte("1"),
						Value: []byte("1"),
					},
				},
				{
					partition: 1,
					msg: &mq.Message{
						Key:   []byte("2"),
						Value: []byte("2"),
					},
				},
				{
					partition: 2,
					msg: &mq.Message{
						Key:   []byte("3"),
						Value: []byte("3"),
					},
				},
				{
					partition: 0,
					msg: &mq.Message{
						Key:   []byte("4"),
						Value: []byte("4"),
					},
				},
				{
					partition: 1,
					msg: &mq.Message{
						Key:   []byte("5"),
						Value: []byte("5"),
					},
				},
				{
					partition: 2,
					msg: &mq.Message{
						Key:   []byte("6"),
						Value: []byte("6"),
					},
				},
			},
			wantVal: []*mq.Message{
				{
					Value:     []byte("1"),
					Key:       []byte("1"),
					Topic:     "test_topic4",
					Partition: 0,
				},
				{
					Value:     []byte("2"),
					Key:       []byte("2"),
					Topic:     "test_topic4",
					Partition: 1,
				},
				{
					Value:     []byte("3"),
					Key:       []byte("3"),
					Topic:     "test_topic4",
					Partition: 2,
				},
				{
					Value:     []byte("4"),
					Key:       []byte("4"),
					Topic:     "test_topic4",
					Partition: 0,
				},
				{
					Value:     []byte("5"),
					Key:       []byte("5"),
					Topic:     "test_topic4",
					Partition: 1,
				},
				{
					Value:     []byte("6"),
					Key:       []byte("6"),
					Topic:     "test_topic4",
					Partition: 2,
				},
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(b.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
		},
	}
	for _, tc := range testcases {
		b.T().Run(tc.name, func(t *testing.T) {
			mqm := b.mqCreator.InitMq()
			err := mqm.Topic(context.Background(), tc.topic, int(tc.partitions))
			require.NoError(t, err)
			p, err := mqm.Producer(tc.topic)
			require.NoError(t, err)
			c, err := mqm.Consumer(tc.topic, "1")
			require.NoError(t, err)
			consumers := []mq.Consumer{
				c,
			}
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			var wg sync.WaitGroup
			locker := sync.RWMutex{}
			for _, c := range consumers {
				newc := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					msgs := tc.consumerFunc(newc)
					locker.Lock()
					ans = append(ans, msgs...)
					locker.Unlock()
				}()
			}
			for _, msg := range tc.input {
				_, err := p.ProduceWithPartition(context.Background(), msg.msg, msg.partition)
				require.NoError(t, err)
			}
			time.Sleep(5 * time.Second)
			err = mqm.Close()
			require.NoError(t, err)
			wg.Wait()
			assert.ElementsMatch(t, tc.wantVal, genWithPartitionMsg(ans))
		})
	}
}

func genMsg(msgs []*mq.Message) []*mq.Message {
	for index, _ := range msgs {
		msgs[index].Partition = 0
		msgs[index].Offset = 0
		msgs[index].Header = nil
	}
	return msgs
}

func genWithPartitionMsg(msgs []*mq.Message) []*mq.Message {
	for index, _ := range msgs {
		msgs[index].Offset = 0
		msgs[index].Header = nil
	}
	return msgs
}

func getMsgMap(msgs []*mq.Message) map[string][]*mq.Message {
	wantMap := make(map[string][]*mq.Message, 10)
	for _, val := range msgs {
		_, ok := wantMap[string(val.Key)]
		if !ok {
			wantMap[string(val.Key)] = append([]*mq.Message{}, val)
		} else {
			wantMap[string(val.Key)] = append(wantMap[string(val.Key)], val)
		}
	}
	return wantMap
}
