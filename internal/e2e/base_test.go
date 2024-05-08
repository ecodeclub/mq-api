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
	"log"
	"sync"
	"testing"
	"time"

	"github.com/ecodeclub/mq-api/internal/errs"

	"github.com/ecodeclub/mq-api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

func NewTestSuite(creator MQCreator) *TestSuite {
	return &TestSuite{
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

func (b *TestSuite) newProducersAndConsumers(t *testing.T, topic string, partitions int, p producerInfo, c consumerInfo) ([]mq.Producer, []mq.Consumer) {
	t.Helper()

	_ = b.messageQueue.CreateTopic(context.Background(), topic, partitions)

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
			assert.ErrorIs(t, err, errs.ErrInvalidTopic, invalidTopic)
		}

		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), invalidTopics...))
	})

	t.Run("合法Partitions", func(t *testing.T) {
		t.Parallel()

		partitions := 1
		validTopic := "validPartitions"
		require.NoError(t, b.messageQueue.CreateTopic(context.Background(), validTopic, partitions))
		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), validTopic))
	})

	t.Run("非法Partitions", func(t *testing.T) {
		t.Parallel()

		partitions := -1
		validTopic1 := "invalidPartitions1"
		assert.ErrorIs(t, b.messageQueue.CreateTopic(context.Background(), validTopic1, partitions), errs.ErrInvalidPartition)

		partitions = 0
		validTopic2 := "invalidPartitions2"
		assert.ErrorIs(t, b.messageQueue.CreateTopic(context.Background(), validTopic2, partitions), errs.ErrInvalidPartition)

		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), validTopic1, validTopic2))
	})

	t.Run("重复创建Topic", func(t *testing.T) {
		t.Parallel()

		createdTopic, partitions := "createdTopic", 1
		err := b.messageQueue.CreateTopic(context.Background(), createdTopic, partitions)
		require.NoError(t, err, createdTopic)

		err = b.messageQueue.CreateTopic(context.Background(), createdTopic, partitions)
		require.NoError(t, err)

		require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), createdTopic))
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

	topics := []string{"topic1", "topic2", "topic3", "topic4"}
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

func (b *TestSuite) TestMQ_Producer() {
	t := b.T()
	t.Parallel()

	unknownTopic := "producer_unknownTopic"
	err := b.messageQueue.CreateTopic(context.Background(), unknownTopic, 1)
	require.NoError(t, err)
	require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), unknownTopic))
	// 如果topic不存在会默认创建，不会报错
	notExistTopic := "notExistTopic"
	_, err = b.messageQueue.Producer(notExistTopic)
	require.NoError(t, err)
}

func (b *TestSuite) TestMQ_Consumer() {
	t := b.T()
	t.Parallel()

	topic, groupID := "topic_a", "c1"
	err := b.messageQueue.CreateTopic(context.Background(), topic, 1)
	require.NoError(t, err)
	require.NoError(t, b.messageQueue.DeleteTopics(context.Background(), topic))
	// 如果topic不存在会默认创建，不会报错
	_, err = b.messageQueue.Consumer(topic, groupID)
	require.NoError(t, err)
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

	consumerGroupID := "c1"
	c, err := messageQueue.Consumer(topic11, consumerGroupID)
	require.NoError(t, err)

	err = messageQueue.DeleteTopics(context.Background(), topic11)
	require.NoError(t, err)

	// 调用close方法
	// 多次调用返回结果一致
	require.Equal(t, messageQueue.Close(), messageQueue.Close())

	// 调用producer上的方法会返回ErrProducerIsClosed
	_, err = p.Produce(context.Background(), &mq.Message{})
	require.ErrorIs(t, err, errs.ErrProducerIsClosed)

	_, err = p.ProduceWithPartition(context.Background(), &mq.Message{}, partitions-1)
	require.ErrorIs(t, err, errs.ErrProducerIsClosed)

	// 调用consumer上的方法会返回ErrConsumerIsClosed
	_, err = c.ConsumeChan(context.Background())
	require.ErrorIs(t, err, errs.ErrConsumerIsClosed)

	_, err = c.Consume(context.Background())
	require.ErrorIs(t, err, errs.ErrConsumerIsClosed)

	// 再次调用MQ上的方法会返回ErrMQIsClosed
	err = messageQueue.CreateTopic(context.Background(), topic11, partitions)
	require.ErrorIs(t, err, errs.ErrMQIsClosed)

	_, err = messageQueue.Producer(topic11)
	require.ErrorIs(t, err, errs.ErrMQIsClosed)

	_, err = messageQueue.Consumer(topic11, consumerGroupID)
	require.ErrorIs(t, err, errs.ErrMQIsClosed)

	err = messageQueue.DeleteTopics(context.Background(), topic11)
	require.ErrorIs(t, err, errs.ErrMQIsClosed)
}

func (b *TestSuite) TestProducer_Produce() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic10, partitions := "topic10", 1
		producers, _ := b.newProducersAndConsumers(t, topic10, partitions, producerInfo{Num: 1}, consumerInfo{})

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

	t.Run("单分区_并发生产消息", func(t *testing.T) {
		t.Parallel()

		producerNum := 2
		topic16, partitions := "topic16", 1
		groupID := "c1"
		produceFunc := func(p mq.Producer, message mq.Message, partitions int) error {
			_, err := p.Produce(context.Background(), &message)
			return err
		}
		b.testProduceMessageConcurrently(t, producerNum, produceFunc, topic16, partitions, groupID, false)
	})

	t.Run("多分区_并发生产消息", func(t *testing.T) {
		t.Parallel()

		producerNum := 2
		topic17, partitions := "topic17", 2
		groupID := "c1"

		produceFunc := func(p mq.Producer, message mq.Message, partition int) error {
			_, err := p.Produce(context.Background(), &message)
			return err
		}

		b.testProduceMessageConcurrently(t, producerNum, produceFunc, topic17, partitions, groupID, false)
	})
}

func (b *TestSuite) TestProducer_ProduceWithPartition() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic13, partitions := "topic13", 2
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

	t.Run("多分区_并发发送", func(t *testing.T) {
		t.Parallel()

		producerNum := 2
		topic18, partitions := "topic18", 3
		groupID := "c1"

		produceFunc := func(p mq.Producer, message mq.Message, partition int) error {
			_, err := p.ProduceWithPartition(context.Background(), &message, partition)
			return err
		}

		b.testProduceMessageConcurrently(t, producerNum, produceFunc, topic18, partitions, groupID, true)
	})
}

func (b *TestSuite) testProduceMessageConcurrently(t *testing.T, producerNum int, produceFunc func(p mq.Producer, m mq.Message, partition int) error, topic string, partitions int, groupID string, withSpecifiedPartition bool) {
	producers, consumers := b.newProducersAndConsumers(t, topic, partitions, producerInfo{Num: producerNum}, consumerInfo{Num: partitions, GroupID: groupID})

	sendMessages := newExpectedMessages("kafka", "nsq", "rocket", "go", "rust", "python")

	var eg errgroup.Group

	// 模拟一个或多个生产者,并发发送消息
	// 调整消息数量
	expectedMessages := make([]mq.Message, 0, len(sendMessages)*producerNum)
	for _, p := range producers {
		p := p
		for i, message := range sendMessages {
			partition := i % partitions

			msg := message
			eg.Go(func() error {
				return produceFunc(p, msg, partition)
			})

			// 指定分区
			if withSpecifiedPartition {
				message.Partition = int64(partition)
			}
			expectedMessages = append(expectedMessages, message)
		}
	}

	assert.NoError(t, eg.Wait())

	// 每个分区对应的消费者消费消息
	// 本测试不对消费消息的顺序做验证
	messageChan := make(chan *mq.Message)
	for _, c := range consumers {
		c := c
		go func(c mq.Consumer) {
			ch, err := c.ConsumeChan(context.Background())
			if err != nil {
				return
			}
			for m := range ch {
				messageChan <- m
			}
		}(c)
	}

	actualMessages := make([]mq.Message, 0, len(expectedMessages))
	for {
		message := <-messageChan
		// log.Printf("received message = %#v, topic = %s\n", *message, topic)
		actualMessages = append(actualMessages, *message)
		if len(actualMessages) == cap(actualMessages) {
			break
		}
	}

	assertMessageEqual(t, actualMessages, expectedMessages, withSpecifiedPartition)
}

func (b *TestSuite) TestProducer_Close() {
	t := b.T()
	t.Parallel()

	topic15, partitions := "topic15", 1
	producers, consumers := b.newProducersAndConsumers(t, topic15, partitions, producerInfo{Num: 1}, consumerInfo{Num: 1, GroupID: "c1"})

	p, c := producers[0], consumers[0]

	_, err := p.Produce(context.Background(), &mq.Message{Value: []byte("hello")})
	require.NoError(t, err)
	_, err = p.ProduceWithPartition(context.Background(), &mq.Message{Value: []byte("world")}, partitions-1)
	require.NoError(t, err)

	_, err = c.Consume(context.Background())
	require.NoError(t, err)
	_, err = c.Consume(context.Background())
	require.NoError(t, err)

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
	require.ErrorIs(t, err, errs.ErrProducerIsClosed)
	_, err = p.ProduceWithPartition(context.Background(), &mq.Message{Value: []byte("world")}, partitions-1)
	require.ErrorIs(t, err, errs.ErrProducerIsClosed)
}

func (b *TestSuite) TestConsumer_Close() {
	t := b.T()
	t.Parallel()

	topic5, partitions := "topic5", 1

	_, consumers := b.newProducersAndConsumers(t, topic5, partitions, producerInfo{}, consumerInfo{Num: 1, GroupID: "c1"})

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
	require.ErrorIs(t, err, errs.ErrConsumerIsClosed)

	_, err = c.Consume(context.Background())
	require.ErrorIs(t, err, errs.ErrConsumerIsClosed)
}

func (b *TestSuite) TestConsumer_ConsumeChan() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic6, partitions := "topic6", 1
		consumerNum, groupID := 1, "c1"

		_, consumers := b.newProducersAndConsumers(t, topic6, partitions, producerInfo{}, consumerInfo{Num: consumerNum, GroupID: groupID})

		c := consumers[0]

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		_, err := c.ConsumeChan(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("相同消费者组", func(t *testing.T) {
		t.Parallel()

		t.Run("单分区_分区内顺序消费", func(t *testing.T) {
			t.Parallel()

			topic7, partitions := "topic7", 1
			groupID := "c1"
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				ch, err := c.ConsumeChan(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				message := <-ch
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic7, partitions, groupID)
		})

		t.Run("多分区_分区内顺序消费_分区间无序_消费者个数必须与分区数相等", func(t *testing.T) {
			t.Parallel()

			topic19, partitions := "topic19", 3
			groupID := "c1"
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				ch, err := c.ConsumeChan(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				message := <-ch
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic19, partitions, groupID)
		})
	})

	t.Run("不同消费者组", func(t *testing.T) {
		t.Parallel()

		t.Run("单分区_分区内顺序消费_消费组间互不影响", func(t *testing.T) {
			t.Parallel()

			topic20, partitions := "topic20", 1
			groupIDs := []string{"c1", "c2"}
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				ch, err := c.ConsumeChan(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				message := <-ch
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic20, partitions, groupIDs...)
		})

		t.Run("多分区_分区内顺序消费_分区间无序_消费组间互不影响_消费者个数必须与分区数相等", func(t *testing.T) {
			t.Parallel()

			topic21, partitions := "topic21", 3
			groupIDs := []string{"c1", "c2", "c3"}
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				ch, err := c.ConsumeChan(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				message := <-ch
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic21, partitions, groupIDs...)
		})
	})
}

func (b *TestSuite) testConsumeSequentially(t *testing.T, consumeFunc func(c mq.Consumer) (mq.Message, error), topic string, partitions int, groupIDs ...string) {
	producers, _ := b.newProducersAndConsumers(t, topic, partitions, producerInfo{Num: 1}, consumerInfo{})
	p := producers[0]
	sendMessage := newExpectedMessages("0", "1", "2", "3", "4", "5")
	expectedMessageMap := make(map[int64][]mq.Message)

	// 并发发送多个信息,
	var eg errgroup.Group
	for i, message := range sendMessage {
		partition := i % partitions
		msg := message
		eg.Go(func() error {
			_, err := p.ProduceWithPartition(context.Background(), &msg, partition)
			return err
		})
		message.Partition = int64(partition)
		expectedMessageMap[message.Partition] = append(expectedMessageMap[message.Partition], message)
	}

	require.NoError(t, eg.Wait())

	for _, groupID := range groupIDs {
		// log.Printf("topic = %s, groupID = %s\n\n", topic, groupID)

		b.testConsumeSequentiallyInSameConsumeGroup(t, consumeFunc, topic, partitions, groupID, expectedMessageMap)
	}
}

func (b *TestSuite) testConsumeSequentiallyInSameConsumeGroup(t *testing.T, consumeFunc func(c mq.Consumer) (mq.Message, error), topic string, partitions int, groupID string, expectedMessageMap map[int64][]mq.Message) {
	_, consumers := b.newProducersAndConsumers(t, topic, partitions, producerInfo{}, consumerInfo{Num: partitions, GroupID: groupID})
	// sendMessage := newExpectedMessages("redis", "mysql", "mongodb", "es", "mq", "gin")

	// log.Printf("topic = %s, partitions = %d, groupID = %s, consumerNum = %d, %#v\n\n", topic, partitions, groupID, len(consumers), expectedMessageMap)
	var eg errgroup.Group
	for _, c := range consumers {
		c := c
		eg.Go(func() error {
			n, partition := 0, int64(0)
			actualMessages := make([]mq.Message, 0)
			for {
				actualMessage, err := consumeFunc(c)
				if err != nil {
					return err
				}
				actualMessages = append(actualMessages, actualMessage)
				n++
				if n == len(expectedMessageMap[actualMessage.Partition]) {
					partition = actualMessage.Partition
					break
				}
			}
			// groupID := groupID
			// log.Printf("%s, %#v\n\n", groupID, actualMessages)
			assertMessageEqual(t, actualMessages, expectedMessageMap[partition], true)
			return nil
		})
	}

	require.NoError(t, eg.Wait())
}

func (b *TestSuite) TestConsumer_Consume() {
	t := b.T()
	t.Parallel()

	t.Run("调用超时_返回错误", func(t *testing.T) {
		t.Parallel()

		topic12, partitions := "topic12", 1
		consumerNum, groupID := 1, "c1"

		_, consumers := b.newProducersAndConsumers(t, topic12, partitions, producerInfo{}, consumerInfo{Num: consumerNum, GroupID: groupID})

		c := consumers[0]

		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()

		_, err := c.Consume(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("相同消费者组", func(t *testing.T) {
		t.Parallel()

		t.Run("单分区_分区内顺序消费", func(t *testing.T) {
			t.Parallel()

			topic22, partitions := "topic22", 1
			groupID := "c1"
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				message, err := c.Consume(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic22, partitions, groupID)
		})

		t.Run("多分区_分区内顺序消费_分区间无序_消费者个数必须与分区数相等", func(t *testing.T) {
			t.Parallel()

			topic23, partitions := "topic23", 3
			groupID := "c1"
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				message, err := c.Consume(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic23, partitions, groupID)
		})
	})

	t.Run("不同消费者组", func(t *testing.T) {
		t.Parallel()

		t.Run("单分区_分区内顺序消费_消费组间互不影响", func(t *testing.T) {
			t.Parallel()

			topic24, partitions := "topic24", 1
			groupIDs := []string{"c1", "c2"}
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				message, err := c.Consume(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic24, partitions, groupIDs...)
		})

		t.Run("多分区_分区内顺序消费_分区间无序_消费组间互不影响_消费者个数必须与分区数相等", func(t *testing.T) {
			t.Parallel()

			topic25, partitions := "topic25", 3
			groupIDs := []string{"c1", "c2", "c3"}
			consumeFunc := func(c mq.Consumer) (mq.Message, error) {
				message, err := c.Consume(context.Background())
				if err != nil {
					return mq.Message{}, err
				}
				return *message, err
			}
			b.testConsumeSequentially(t, consumeFunc, topic25, partitions, groupIDs...)
		})
	})
}

func newExpectedMessages(messages ...string) []mq.Message {
	res := make([]mq.Message, 0, len(messages))
	for _, message := range messages {
		m := mq.Message{Value: []byte(message)}
		res = append(res, m)
	}
	return res
}

func assertMessageEqual(t *testing.T, actualMessages []mq.Message, expectedMessages []mq.Message, withSpecifiedPartition bool) {
	t.Helper()
	require.Equal(t, len(expectedMessages), len(actualMessages))
	for i := range actualMessages {
		actualMessages[i].Topic = ""
		actualMessages[i].Offset = 0
		actualMessages[i].Header = nil
		if !withSpecifiedPartition {
			actualMessages[i].Partition = 0
		}
	}
	require.ElementsMatch(t, actualMessages, expectedMessages)
	// log.Println("assertMessageEqual true")
}
