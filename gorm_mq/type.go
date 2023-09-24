package gorm_mq

import (
	"github.com/ecodeclub/mq-api/gorm_mq/getter/poll"
)

type ProducerGetter interface {
	Get(key string) int64
}

type ConsumerBalancer interface {
	// Balance 返回的是每个消费者他的分区
	Balance(partition int, consumers int) [][]int
}

type NewProducerGetter func(partition int) ProducerGetter

func NewGetter(partition int) ProducerGetter {
	return &poll.Getter{
		Partition: partition,
	}
}
