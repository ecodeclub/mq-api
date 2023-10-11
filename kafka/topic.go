package kafka

type topic struct {
	Name      string
	Partition int
}

func newTopic(name string, partition int) *topic {
	return &topic{
		Name:      name,
		Partition: partition,
	}
}
