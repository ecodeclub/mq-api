package gorm_mq

type ProducerGetter interface {
	Get() int64
}

type ConsumerBalancer interface {
	// Balance 返回的是每个消费者他的分区
	Balance(partition int, consumers int) [][]int
}
