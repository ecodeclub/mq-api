package e2e

import (
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka"
)

type KafkaSuite struct {
	brokers []string
}

func NewKafkaSuite(brokers []string) *KafkaSuite {
	return &KafkaSuite{
		brokers: brokers,
	}
}
func (k *KafkaSuite) Init() mq.MQ {
	kafkaMq, err := kafka.NewMQ(k.brokers)
	if err != nil {
		panic(err)
	}
	return kafkaMq
}
