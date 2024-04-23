package memory

import (
	"github.com/ecodeclub/mq-api"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Partition(t *testing.T) {
	p := NewPartition()
	for i := 0; i < 5; i++ {
		msg := &mq.Message{Partition: int64(i)}
		p.append(msg)
	}
	msgs := p.getBatch(2, 2)
	assert.Equal(t, []*mq.Message{
		{
			Partition: 2,
			Offset:    2,
		},
		{
			Partition: 3,
			Offset:    3,
		},
	}, msgs)
	msgs = p.getBatch(2, 5)
	assert.Equal(t, []*mq.Message{
		{
			Partition: 2,
			Offset:    2,
		},
		{
			Partition: 3,
			Offset:    3,
		},
		{
			Partition: 4,
			Offset:    4,
		},
	}, msgs)

}
