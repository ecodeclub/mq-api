package e2e

import (
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

// 同一topic 消费组，重复消费
// 同一topic 消费组内，组内相互竞争
// 同一分区 按顺序消费
// 生产者

type KafkaSuite struct {
	BaseSuite
	brokers []string
}

func (k *KafkaSuite) InitMq() mq.MQ {
	kafkaMq, err := kafka.NewCustomMq(k.brokers)
	if err != nil {
		panic(err)
	}
	return kafkaMq
}

func NewKafkaSuite(brokers []string) *KafkaSuite {
	k := &KafkaSuite{
		BaseSuite: BaseSuite{},
		brokers:   brokers,
	}
	k.mqCreator = k
	return k

}
func (k *KafkaSuite) SetupTest() {
	kafkaMq, err := kafka.NewCustomMq(k.brokers)
	if err != nil {
		panic(err)
	}
	topics := []string{
		"test_topic",
		"test_topic1",
		"test_topic2",
		"test_topic3",
		"test_topic4",
	}
	err = kafkaMq.ClearKafka(topics)
	require.NoError(k.T(), err)
}
func (k *KafkaSuite) TearDownTest() {
	kafkaMq, err := kafka.NewCustomMq(k.brokers)
	if err != nil {
		panic(err)
	}
	topics := []string{
		"test_topic",
		"test_topic1",
		"test_topic2",
	}
	err = kafkaMq.ClearKafka(topics)
	require.NoError(k.T(), err)
}

func (k *KafkaSuite) TestMSMQConsumer_DiffGroup() {
	k.BaseSuite.TestMSMQConsumer_DiffGroup()
}

func (k *KafkaSuite) TestMSMQConsumer_PartitionSort() {
	k.BaseSuite.TestMSMQConsumer_PartitionSort()
}

func (k *KafkaSuite) TestMSMQProducer_ProduceWithPartition() {
	k.BaseSuite.TestMSMQProducer_ProduceWithPartition()
}

func TestMq(t *testing.T) {
	suite.Run(t, NewKafkaSuite([]string{"127.0.0.1:9092"}))
}
