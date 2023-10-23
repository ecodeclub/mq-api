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

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/mqerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

type MQCreator interface {
	Create() mq.MQ
	Ping(ctx context.Context) error
}

type TestSuite struct {
	suite.Suite
	name         string
	mqCreator    MQCreator
	messageQueue mq.MQ
}

type producerInfo struct {
	Num int
}

type consumerInfo struct {
	Num     int
	GroupID string
}

func NewTestSuite(name string, creator MQCreator) *TestSuite {
	return &TestSuite{
		name:      name,
		mqCreator: creator,
	}
}

func (b *TestSuite) SetupSuite() {
	for {
		if err := b.mqCreator.Ping(context.Background()); err == nil {
			break
		}
		log.Println("连接失败,重试中.....")
		time.Sleep(time.Second * 3)
	}
	b.messageQueue = b.mqCreator.Create()
}

func (b *TestSuite) topic(name string) string {
	return b.name + "-" + name
}

func (b *TestSuite) newProducersAndConsumers(t *testing.T, topic string, partitions int, p producerInfo, c consumerInfo) ([]mq.Producer, []mq.Consumer) {
	t.Helper()

	err := b.messageQueue.CreateTopic(context.Background(), topic, partitions)
	require.NoError(t, err)

	producers := make([]mq.Producer, 0, p.Num)
	for i := 0; i < p.Num; i++ {
		pp, err := b.messageQueue.Producer(topic)
		require.NoError(t, err)
		producers = append(producers, pp)
	}

	consumers := make([]mq.Consumer, 0, c.Num)
	for i := 0; i < c.Num; i++ {
		cc, err := b.messageQueue.Consumer(topic, c.GroupID)
		require.NoError(t, err)
		consumers = append(consumers, cc)
	}

	t.Cleanup(func() {
		for _, p := range producers {
			require.NoError(t, p.Close())
		}
		for _, c := range consumers {
			require.NoError(t, c.Close())
		}
	})

	return producers, consumers
}

func (b *TestSuite) TestMQ_CreateTopic() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		validTopic, partitions := "CreateTopic", 1
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.ErrorIs(t, b.messageQueue.CreateTopic(ctx, validTopic, partitions), context.Canceled)
	})

	t.Run("合法Topic", func(t *testing.T) {
		t.Parallel()

		validTopics := []string{
			"topicName",
			"topicName1",
			"topicName-2",
			"topicName_3",
			"topicName.4",
			"my-topic",
			"other_.-topic",
			"prefix.sub-topic",
		}
		partitions := 2

		for _, validTopic := range validTopics {
			err := b.messageQueue.CreateTopic(context.Background(), validTopic, partitions)
			assert.NoError(t, err, validTopic)
		}

		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), validTopics...))
	})

	t.Run("非法Topic", func(t *testing.T) {
		t.Parallel()

		invalidTopics := []string{
			"a/b", "a,b", "a*b",
			"0", "1a", "2-", "2.", "2_", "3-1", "3.1", "3_1",
			"-", ".", "_", "-1", ".1", "_1",
			"",               // 空字符串
			".invalid.topic", // . 作为开头
			"-invalid-topic", // - 作为开头
			"_invalid_topic", // _ 作为开头
			"123-topic",      // 数字开头
			"topicName-",     // - 作为结尾
			"topicName.",     // . 作为结尾
			"topicName_",     // _ 作为结尾
			"topic Name",     // 包含空格
			"topic!",         // 包含非法字符
			"topic-name-is-too-long-topic-name-is-too-long-topic-name-is-too-long", // 超过最大长度
		}
		partitions := 2

		for _, invalidTopic := range invalidTopics {
			err := b.messageQueue.CreateTopic(context.Background(), invalidTopic, partitions)
			assert.ErrorIs(t, err, mqerr.ErrInvalidTopic, invalidTopic)
		}

		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), invalidTopics...))
	})

	t.Run("合法Partitions", func(t *testing.T) {
		t.Parallel()

		partitions := 1
		validPartitionsTopic := "validPartitions"
		require.NoError(t, b.messageQueue.CreateTopic(context.Background(), validPartitionsTopic, partitions))
		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), validPartitionsTopic))
	})

	t.Run("非法Partitions", func(t *testing.T) {
		t.Parallel()

		partitions := -1
		invalidPartitionsTopic1 := "invalidPartitions1"
		assert.ErrorIs(t, b.messageQueue.CreateTopic(context.Background(), invalidPartitionsTopic1, partitions), mqerr.ErrInvalidPartition)

		partitions = 0
		invalidPartitionsTopic2 := "invalidPartitions2"
		assert.Error(t, b.messageQueue.CreateTopic(context.Background(), invalidPartitionsTopic2, partitions), mqerr.ErrInvalidPartition)

		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), invalidPartitionsTopic1, invalidPartitionsTopic2))
	})
}

func (b *TestSuite) TestMQ_DeleteTopics() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		validTopic := "DeleteTopics"
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.ErrorIs(t, b.messageQueue.DeleteTopics(ctx, validTopic), context.Canceled)
	})

	t.Run("删除未知Topic_不返回错误", func(t *testing.T) {
		t.Parallel()

		topics := []string{"unknownTopic1", "unknownTopic2"}
		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), topics...))
	})
}

func (b *TestSuite) TestMQ_CreateTopicAndDeleteTopics() {
	t := b.T()
	t.Parallel()

	var eg errgroup.Group

	topics := []string{b.topic("topic1"), b.topic("topic2"), b.topic("topic3"), b.topic("topic4")}
	partitions := 2

	// 并发创建多个Topic
	for _, topic := range topics {
		topic := topic
		eg.Go(func() error {
			return b.messageQueue.CreateTopic(context.Background(), topic, partitions)
		})
	}
	require.NoError(t, eg.Wait())

	// 并发删除多个Topic
	for _, topic := range topics {
		topic := topic
		eg.Go(func() error {
			return b.messageQueue.DeleteTopics(context.Background(), topic)
		})
	}
	require.NoError(t, eg.Wait())
}

func (b *TestSuite) TestMQ_Close() {
	t := b.T()
	t.Parallel()

	topic11 := "topic11"
	partitions := 4
	messageQueue := b.mqCreator.Create()

	// Close之前一切正常
	err := messageQueue.CreateTopic(context.Background(), topic11, partitions)
	require.NoError(t, err)

	p, err := messageQueue.Producer(topic11)
	require.NoError(t, err)

	consumerGroupID := "1"
	c, err := messageQueue.Consumer(topic11, consumerGroupID)
	require.NoError(t, err)

	err = messageQueue.DeleteTopics(context.Background(), topic11)
	require.NoError(t, err)

	// 调用close方法
	// 多次调用返回结果一致
	require.Equal(t, messageQueue.Close(), messageQueue.Close())

	// 调用producer上的方法会返回ErrProducerIsClosed
	_, err = p.Produce(context.Background(), &mq.Message{})
	require.ErrorIs(t, err, mqerr.ErrProducerIsClosed)

	_, err = p.ProduceWithPartition(context.Background(), &mq.Message{}, partitions-1)
	require.ErrorIs(t, err, mqerr.ErrProducerIsClosed)

	// 调用consumer上的方法会返回ErrConsumerIsClosed
	_, err = c.ConsumeChan(context.Background())
	require.ErrorIs(t, err, mqerr.ErrConsumerIsClosed)

	_, err = c.Consume(context.Background())
	require.ErrorIs(t, err, mqerr.ErrConsumerIsClosed)

	// 再次调用MQ上的方法会返回ErrMQIsClosed
	err = messageQueue.CreateTopic(context.Background(), topic11, partitions)
	require.ErrorIs(t, err, mqerr.ErrMQIsClosed)

	_, err = messageQueue.Producer(topic11)
	require.ErrorIs(t, err, mqerr.ErrMQIsClosed)

	_, err = messageQueue.Consumer(topic11, consumerGroupID)
	require.ErrorIs(t, err, mqerr.ErrMQIsClosed)

	err = messageQueue.DeleteTopics(context.Background(), topic11)
	require.ErrorIs(t, err, mqerr.ErrMQIsClosed)
}

func (b *TestSuite) TestProducer_Produce() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic10, partitions := b.topic("topic10"), 1
		producerNum := 1
		producers, _ := b.newProducersAndConsumers(t, topic10, partitions, producerInfo{Num: producerNum}, consumerInfo{})

		p := producers[0]

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		_, err := p.Produce(ctx, &mq.Message{Value: []byte("hello")})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("消息格式非法", func(t *testing.T) {
		// 验证mq.Message中哪些字段是应该在produce阶段必须设置的,哪些是可选的,哪些是不能设置的
		t.Skip()
		t.Parallel()
	})
}

func (b *TestSuite) TestProducer_ProduceWithPartition() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic13, partitions := b.topic("topic13"), 2
		producers, _ := b.newProducersAndConsumers(t, topic13, partitions, producerInfo{Num: 1}, consumerInfo{})

		p := producers[0]

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		_, err := p.ProduceWithPartition(ctx, &mq.Message{Value: []byte("hello")}, partitions-1)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("消息格式非法", func(t *testing.T) {
		// 验证mq.Message中哪些字段是应该在produce阶段必须设置的,哪些是可选的,哪些是不能设置的
		t.Skip()
		t.Parallel()
	})

	t.Run("分区ID非法_返回错误", func(t *testing.T) {
		t.Parallel()

		topic14, partitions := b.topic("topic14"), 2
		producers, _ := b.newProducersAndConsumers(t, topic14, partitions, producerInfo{Num: 1}, consumerInfo{})

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		p := producers[0]

		_, err := p.ProduceWithPartition(ctx, &mq.Message{Value: []byte("hello")}, partitions)
		require.ErrorIs(t, err, mqerr.ErrInvalidPartition)

		_, err = p.ProduceWithPartition(ctx, &mq.Message{Value: []byte("hello")}, partitions+1)
		require.ErrorIs(t, err, mqerr.ErrInvalidPartition)

		_, err = p.ProduceWithPartition(ctx, &mq.Message{Value: []byte("hello")}, -1)
		require.ErrorIs(t, err, mqerr.ErrInvalidPartition)
	})
}

func (b *TestSuite) TestProducer_Close() {
	t := b.T()
	t.Parallel()

	topic15, partitions := b.topic("topic15"), 1
	producers, consumers := b.newProducersAndConsumers(t, topic15, partitions, producerInfo{Num: 1}, consumerInfo{Num: 1})

	p, c := producers[0], consumers[0]

	_, err := p.Produce(context.Background(), &mq.Message{Value: []byte("hello")})
	require.NoError(t, err)
	_, err = p.ProduceWithPartition(context.Background(), &mq.Message{Value: []byte("world")}, partitions-1)
	require.NoError(t, err)

	_, err = c.Consume(context.Background())
	require.NoError(t, err)
	// log.Println("TestProducer_Close", m1)
	_, err = c.Consume(context.Background())
	require.NoError(t, err)
	// log.Println("TestProducer_Close", m2)

	// 并发调用Close
	n := 3
	closeErrChan := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(p mq.Producer) {
			closeErrChan <- p.Close()
			wg.Done()
		}(p)
	}

	wg.Wait()
	close(closeErrChan)

	// 并发调用Close,所有返回的error应该相同
	err = <-closeErrChan
	for e := range closeErrChan {
		require.Equal(t, err, e)
	}

	// 调用Close后
	_, err = p.Produce(context.Background(), &mq.Message{Value: []byte("hello")})
	require.ErrorIs(t, err, mqerr.ErrProducerIsClosed)
	_, err = p.ProduceWithPartition(context.Background(), &mq.Message{Value: []byte("world")}, partitions-1)
	require.ErrorIs(t, err, mqerr.ErrProducerIsClosed)
}

func (b *TestSuite) TestConsumer_Close() {
	t := b.T()
	t.Parallel()

	topic5, partitions := b.topic("topic5"), 1

	_, consumers := b.newProducersAndConsumers(t, topic5, partitions, producerInfo{}, consumerInfo{Num: 1})

	c := consumers[0]

	messageChan, err := c.ConsumeChan(context.Background())
	require.NoError(t, err)

	n := 3
	closeErrChan := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(c mq.Consumer) {
			closeErrChan <- c.Close()
			wg.Done()
		}(c)
	}

	wg.Wait()
	close(closeErrChan)

	// 并发调用Close,所有返回的error应该相同
	err = <-closeErrChan
	for e := range closeErrChan {
		require.Equal(t, err, e)
	}

	// 已经获得的messageChan应该被关闭
	_, ok := <-messageChan
	require.False(t, ok)

	// 再调用Consumer上的其他方法将返回error
	_, err = c.ConsumeChan(context.Background())
	require.ErrorIs(t, err, mqerr.ErrConsumerIsClosed)

	_, err = c.Consume(context.Background())
	require.ErrorIs(t, err, mqerr.ErrConsumerIsClosed)
}

func (b *TestSuite) TestConsumer_ConsumeChan() {
	// 同一Topic:
	// Close()前, 1) 并发获取Chan, 获取到消息的内容与produce的一样
	//            2) 单个Consumer, 超时返回错误
	// Close()后, 并发调用ConsumChan, 返回关闭的Chan + 返回mqerr.ErrConsumerIsClosed, 详见TestConsumer_Close()
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic6, partitions := b.topic("topic6"), 1
		consumerNum, groupID := 1, "c1"

		_, consumers := b.newProducersAndConsumers(t, topic6, partitions, producerInfo{}, consumerInfo{Num: consumerNum, GroupID: groupID})

		consumer := consumers[0]

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		_, err := consumer.ConsumeChan(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("同一Topic_单个分区", func(t *testing.T) {
		t.Parallel()

		t.Run("单个消费者组", func(t *testing.T) {
			t.Parallel()

			t.Run("组内单个消费者顺序消费", func(t *testing.T) {
				t.Parallel()

				topic7 := b.topic("topic7")
				partitions := 1
				groupID := "c1"

				err := b.messageQueue.CreateTopic(context.Background(), topic7, partitions)
				require.NoError(t, err)

				p, err := b.messageQueue.Producer(topic7)
				require.NoError(t, err)

				c, err := b.messageQueue.Consumer(topic7, groupID)
				require.NoError(t, err)

				t.Cleanup(func() {
					require.NoError(t, p.Close())
					require.NoError(t, c.Close())
				})

				expectedValues := []string{"hello", "world"}

				for _, value := range expectedValues {
					_, err := p.Produce(context.Background(), &mq.Message{Value: []byte(value)})
					require.NoError(t, err, value)
				}

				messageChan, err := c.ConsumeChan(context.Background())
				require.NoError(t, err)

				// 验证收到的消息 —— 各个字段都要验证
				for _, expectedValue := range expectedValues {
					message := <-messageChan
					require.NoError(t, err)
					require.Equal(t, topic7, message.Topic)
					require.Equal(t, expectedValue, string(message.Value))
					require.GreaterOrEqual(t, message.Partition, int64(0))
					require.Less(t, message.Partition, int64(partitions))
				}
			})

			t.Run("组内多个消费者竞争消费", func(t *testing.T) {
				t.Parallel()

				topic8, partitions := b.topic("topic8"), 3
				groupID := "c1"

				producers, consumers := b.newProducersAndConsumers(t, topic8, partitions, producerInfo{Num: 1}, consumerInfo{Num: 3, GroupID: groupID})

				p, c1, c2, c3 := producers[0], consumers[0], consumers[1], consumers[2]

				expectedValues := [][]string{{"go", "python", "rust"}, {"kafka", "rabbitmq", "rocketmq"}, {"redis", "mysql", "mongodb"}}
				var eg errgroup.Group
				mp := &sync.Map{}
				for i, values := range expectedValues {
					mp.Store(int64(i), make([]string, 0, len(values)))

					for _, value := range values {
						value := value
						partition := i
						eg.Go(func() error {
							_, err := p.ProduceWithPartition(context.Background(), &mq.Message{Value: []byte(value)}, partition)
							require.NoError(t, err, partition)
							return err
						})
					}
				}

				for _, c := range []mq.Consumer{c1, c2, c3} {
					c := c
					eg.Go(func() error {
						messageChan, err := c.ConsumeChan(context.Background())
						require.NoError(t, err)
						for {
							message := <-messageChan
							v, ok := mp.Load(message.Partition)
							require.True(t, ok)

							values, ok := v.([]string)
							require.True(t, ok)

							values = append(values, string(message.Value))
							mp.Store(message.Partition, values)

							if len(values) == cap(values) {
								return nil
							}
						}
					})
				}

				require.NoError(t, eg.Wait())

				for partition, expectedValue := range expectedValues {
					v, ok := mp.Load(int64(partition))
					require.True(t, ok)
					actualValue, ok := v.([]string)
					require.True(t, ok)
					require.ElementsMatch(t, actualValue, expectedValue)
				}
			})
		})

		t.Run("多个消费者组", func(t *testing.T) {
			t.Skip()
			t.Parallel()

			t.Run("组内单个消费者顺序消费_组间互不干扰可重复消费", func(t *testing.T) {
			})

			t.Run("组内多个消费者竞争消费_组间互不干扰可重复消费", func(t *testing.T) {
			})
		})
	})
}

func (b *TestSuite) TestConsumer_Consume() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic12, partitions := b.topic("topic12"), 1
		consumerNum, groupID := 1, "c1"

		_, consumers := b.newProducersAndConsumers(t, topic12, partitions, producerInfo{}, consumerInfo{Num: consumerNum, GroupID: groupID})

		consumer := consumers[0]

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		_, err := consumer.Consume(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	// 同一Topic, 单分区:
	// Close()前
	// 		1)并发获取,正常获取消息, 消息与produce的一样
	// 		2) 单个Consumer, 超时返回错误
	// 同一Topic, 多分区:
	//      1)多个并发获取, 正常获取消息, 消息与produce的一样
	//      2) 多个
	// Close()后, 并发调用的Consume后 返回mqerr.ErrConsumerIsClosed, 详见TestConsumer_Close()
}

/*
// 测试消费组

	func (b *TestSuite) TestConsumer_ConsumerGroup() {
		t := b.T()
		testcases := []struct {
			name        string
			topic       string
			partitions  int64
			input       []*mq.Message
			consumers   func(mqm mq.MQ) []mq.Consumer
			consumeFunc func(c mq.Consumer, ch chan *mq.Message)
			wantVal     []*mq.Message
		}{
			// {
			// 	name:       "多个消费组订阅同一个Topic,消费组之间可以重复消费消息",
			// 	topic:      "test_topic1",
			// 	partitions: 1,
			// 	input: []*mq.Message{
			// 		{
			// 			Value: []byte("1"),
			// 			Key:   []byte("1"),
			// 		},
			// 		{
			// 			Value: []byte("2"),
			// 			Key:   []byte("2"),
			// 		},
			// 	},
			// 	consumers: func(mqm mq.MQ) []mq.Consumer {
			// 		c11, err := mqm.Consumer("test_topic1", "c1")
			// 		require.NoError(b.T(), err)
			// 		c21, err := mqm.Consumer("test_topic1", "c2")
			// 		require.NoError(b.T(), err)
			// 		return []mq.Consumer{
			// 			c11,
			// 			c21,
			// 		}
			// 	},
			// 	consumeFunc: func(c mq.Consumer, ch chan *mq.Message) {
			// 		msgCh, err := c.ConsumeChan(context.Background())
			// 		require.NoError(b.T(), err)
			// 		for val := range msgCh {
			// 			ch <- val
			// 		}
			// 	},
			// 	wantVal: []*mq.Message{
			// 		{
			// 			Value: []byte("1"),
			// 			Key:   []byte("1"),
			// 			Topic: "test_topic1",
			// 		},
			// 		{
			// 			Value: []byte("2"),
			// 			Key:   []byte("2"),
			// 			Topic: "test_topic1",
			// 		},
			// 		{
			// 			Value: []byte("1"),
			// 			Key:   []byte("1"),
			// 			Topic: "test_topic1",
			// 		},
			// 		{
			// 			Value: []byte("2"),
			// 			Key:   []byte("2"),
			// 			Topic: "test_topic1",
			// 		},
			// 	},
			// },
			{
				name:       "同一消费者组内,各个消费者竞争消费消息",
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
				consumeFunc: func(c mq.Consumer, ch chan *mq.Message) {
					msgCh, err := c.ConsumeChan(context.Background())
					require.NoError(t, err)
					for val := range msgCh {
						ch <- val
					}
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
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				// t.Parallel()

				err := b.messageQueue.CreateTopic(context.Background(), tc.topic, int(tc.partitions))
				require.NoError(t, err)
				p, err := b.messageQueue.Producer(tc.topic)
				require.NoError(t, err)
				consumers := tc.consumers(b.messageQueue)
				var wg sync.WaitGroup
				ch := make(chan *mq.Message, 64)
				for _, c := range consumers {
					newc := c
					wg.Add(1)
					go func() {
						defer wg.Done()
						tc.consumeFunc(newc, ch)
					}()
				}
				for _, msg := range tc.input {
					_, err := p.Produce(context.Background(), msg)
					require.NoError(t, err)
				}
				ans := make([]*mq.Message, 0)
				for {
					log.Println("for .....", len(ans))
					ans = append(ans, <-ch)
					if len(ans) == len(tc.wantVal) {
						err = closeConsumerAndProducer(consumers, []mq.Producer{p})
						require.NoError(t, err)
						break
					}
				}

				log.Println("before wait")
				wg.Wait()
				ansMsg := genMsg(ans, false)
				assert.ElementsMatch(t, tc.wantVal, ansMsg)
			})
		}
	}

// 测试同一partition下的顺序

	func (b *TestSuite) TestMQConsumer_OrderOfMessagesWithinAPartition() {
		testcases := []struct {
			name         string
			topic        string
			partitions   int64
			input        []*mq.Message
			consumers    func(mqm mq.MQ) []mq.Consumer
			consumerFunc func(c mq.Consumer, ch chan *mq.Message)
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
						Value: []byte("5"),
					},
					{
						Key:   []byte("4"),
						Value: []byte("6"),
					},
					{
						Key:   []byte("4"),
						Value: []byte("7"),
					},
					{
						Key:   []byte("4"),
						Value: []byte("8"),
					},
				},
				consumers: func(mqm mq.MQ) []mq.Consumer {
					c11, err := mqm.Consumer("test_topic3", "c1")
					require.NoError(b.T(), err)
					return []mq.Consumer{
						c11,
					}
				},
				consumerFunc: func(c mq.Consumer, ch chan *mq.Message) {
					msgCh, err := c.ConsumeChan(context.Background())
					require.NoError(b.T(), err)
					for val := range msgCh {
						ch <- val
					}
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
						Value: []byte("5"),
						Key:   []byte("4"),
						Topic: "test_topic3",
					},
					{
						Value: []byte("6"),
						Key:   []byte("4"),
						Topic: "test_topic3",
					},
					{
						Value: []byte("7"),
						Key:   []byte("4"),
						Topic: "test_topic3",
					},
					{
						Value: []byte("8"),
						Key:   []byte("4"),
						Topic: "test_topic3",
					},
				},
			},
		}
		for _, tc := range testcases {
			b.T().Run(tc.name, func(t *testing.T) {
				mqm := b.messageQueue
				err := mqm.CreateTopic(context.Background(), tc.topic, int(tc.partitions))
				require.NoError(t, err)
				p, err := mqm.Producer(tc.topic)
				require.NoError(t, err)
				consumers := tc.consumers(mqm)
				var wg sync.WaitGroup
				ch := make(chan *mq.Message, 64)
				for _, c := range consumers {
					newc := c
					wg.Add(1)
					go func() {
						defer wg.Done()
						tc.consumerFunc(newc, ch)
					}()
				}
				for _, msg := range tc.input {
					_, err := p.ProduceWithPartition(context.Background(), msg, 0)
					require.NoError(t, err)
				}
				for {
					if len(ch) == len(tc.wantVal) {
						err = closeConsumerAndProducer(consumers, []mq.Producer{p})
						require.NoError(t, err)
						break
					}
				}
				wg.Wait()
				ans := getMsgFromChannel(ch)
				ansMsg := genMsg(ans, false)
				wantMap := getMsgMap(tc.wantVal)
				actualMap := getMsgMap(ansMsg)
				assert.Equal(t, wantMap, actualMap)
			})
		}
	}

// 测试发送到指定分区

	func (b *TestSuite) TestMQProducer_ProduceWithSpecifiedPartitionID() {
		type ProducerMsg struct {
			partition int32
			msg       *mq.Message
		}

		testcases := []struct {
			name         string
			topic        string
			partitions   int64
			input        []ProducerMsg
			consumerFunc func(c mq.Consumer, ch chan *mq.Message)
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
						Value:       []byte("1"),
						Key:         []byte("1"),
						Topic:       "test_topic4",
						PartitionID: 0,
					},
					{
						Value:       []byte("2"),
						Key:         []byte("2"),
						Topic:       "test_topic4",
						PartitionID: 1,
					},
					{
						Value:       []byte("3"),
						Key:         []byte("3"),
						Topic:       "test_topic4",
						PartitionID: 2,
					},
					{
						Value:       []byte("4"),
						Key:         []byte("4"),
						Topic:       "test_topic4",
						PartitionID: 0,
					},
					{
						Value:       []byte("5"),
						Key:         []byte("5"),
						Topic:       "test_topic4",
						PartitionID: 1,
					},
					{
						Value:       []byte("6"),
						Key:         []byte("6"),
						Topic:       "test_topic4",
						PartitionID: 2,
					},
				},
				consumerFunc: func(c mq.Consumer, ch chan *mq.Message) {
					msgCh, err := c.ConsumeChan(context.Background())
					require.NoError(b.T(), err)
					for val := range msgCh {
						ch <- val
					}
				},
			},
		}
		for _, tc := range testcases {
			b.T().Run(tc.name, func(t *testing.T) {
				mqm := b.messageQueue
				err := mqm.CreateTopic(context.Background(), tc.topic, int(tc.partitions))
				require.NoError(t, err)
				p, err := mqm.Producer(tc.topic)
				require.NoError(t, err)
				c, err := mqm.Consumer(tc.topic, "1")
				require.NoError(t, err)
				consumers := []mq.Consumer{
					c,
				}
				var wg sync.WaitGroup
				ch := make(chan *mq.Message, 64)
				for _, c := range consumers {
					newc := c
					wg.Add(1)
					go func() {
						defer wg.Done()
						tc.consumerFunc(newc, ch)
					}()
				}
				for _, msg := range tc.input {
					_, err := p.ProduceWithPartition(context.Background(), msg.msg, msg.partition)
					require.NoError(t, err)
				}
				for {
					if len(ch) == len(tc.wantVal) {
						err = closeConsumerAndProducer(consumers, []mq.Producer{p})
						require.NoError(t, err)
						break
					}
				}
				wg.Wait()
				ans := getMsgFromChannel(ch)
				assert.ElementsMatch(t, tc.wantVal, genMsg(ans, true))
			})
		}
	}

// 测试producer调用close

	func (b *TestSuite) TestMQProducer_Close() {
		t := b.T()
		topic := "test_topic5"
		mqm := b.messageQueue
		err := mqm.CreateTopic(context.Background(), topic, 4)
		p, err := mqm.Producer(topic)
		require.NoError(b.T(), err)
		errChan := make(chan error, 10)
		// 开启三个goroutine使用 Produce
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := testProducerClose(p)
				if err != nil {
					errChan <- err
				}
			}()
		}
		// 开启三个goroutine使用 ProducerWithPartition
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := testProducerClose(p)
				if err != nil {
					errChan <- err
				}
			}()
		}
		err = p.Close()
		require.NoError(t, err)
		wg.Wait()
		close(errChan)
		errList := make([]error, 0, len(errChan))
		for val := range errChan {
			errList = append(errList, val)
		}
		for _, e := range errList {
			require.True(t, errors.Is(e, mqerr.ErrProducerIsClosed))
		}
	}

// 测试producer和consumer的并发

	func (b *TestSuite) TestMQConsumer_Consume() {
		testTopic := "test_topic7"
		err := b.messageQueue.CreateTopic(context.Background(), testTopic, 1)
		require.NoError(b.T(), err)
		p, err := b.messageQueue.Producer(testTopic)
		require.NoError(b.T(), err)
		c, err := b.messageQueue.Consumer(testTopic, "c1")
		// 开启3个goroutine使用p的Produce，开启3个goroutine使用p的ProduceWithPartition
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				msgs := genProduceMsg(3, 1, testTopic)
				for _, msg := range msgs {
					_, err := p.Produce(context.Background(), msg)
					require.NoError(b.T(), err)
				}
			}()
			go func() {
				defer wg.Done()
				msgs := genProduceMsg(3, 1, testTopic)
				for _, msg := range msgs {
					_, err := p.ProduceWithPartition(context.Background(), msg, 0)
					require.NoError(b.T(), err)
				}
			}()
		}
		wantVal := genProduceMsg(3, 6, testTopic)
		msgch := make(chan *mq.Message, 64)
		// 开启3个goroutine消费数据
		for i := 0; i < 3; i++ {
			// 测试ConsumeChan
			go func() {
				ch, err := c.ConsumeChan(context.Background())
				require.NoError(b.T(), err)
				for val := range ch {
					msgch <- val
				}
			}()
			// 测试consumer
			go func() {
				msg, err := c.Consume(context.Background())
				require.NoError(b.T(), err)
				msgch <- msg
			}()
		}
		// 等待数据生产完
		wg.Wait()
		// 等待数据处理
		for {
			if len(msgch) == len(wantVal) {
				err = closeConsumerAndProducer([]mq.Consumer{c}, []mq.Producer{p})
				require.NoError(b.T(), err)
				break
			}
		}
		ans := getMsgFromChannel(msgch)
		assert.ElementsMatch(b.T(), wantVal, genMsg(ans, false))
	}
*/
func genMsg(msgs []*mq.Message, hasPartitionID bool) []*mq.Message {
	for index := range msgs {
		if !hasPartitionID {
			msgs[index].Partition = 0
		}
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

func testProducerClose(p mq.Producer) error {
	for {
		_, err := p.Produce(context.Background(), &mq.Message{
			Value: []byte("1"),
		})
		if err != nil {
			return err
		}
	}
}

func closeConsumerAndProducer(consumers []mq.Consumer, producers []mq.Producer) error {
	errList := make([]error, 0, len(consumers)+len(producers))
	for _, c := range consumers {
		err := c.Close()
		if err != nil {
			errList = append(errList, err)
		}
	}
	for _, p := range producers {
		err := p.Close()
		if err != nil {
			errList = append(errList, err)
		}
	}
	return multierr.Combine(errList...)
}

// 生成消息
func genProduceMsg(number int64, receiver int64, topic string) []*mq.Message {
	list := make([]*mq.Message, 0, number)
	for j := 0; j < int(receiver); j++ {
		for i := 0; i < int(number); i++ {
			list = append(list, &mq.Message{
				Value: []byte(fmt.Sprintf("%d", i)),
				Key:   []byte(fmt.Sprintf("%d", i)),
				Topic: topic,
			})
		}
	}

	return list
}

func getMsgFromChannel(ch chan *mq.Message) []*mq.Message {
	list := make([]*mq.Message, 0, len(ch))
	chlen := len(ch)
	for i := 0; i < chlen; i++ {
		list = append(list, <-ch)
	}
	return list
}
