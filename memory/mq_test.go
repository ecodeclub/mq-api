package memory

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

type MemoryMqTestSuite struct {
	suite.Suite
	mq mq.MQ
}

func (m *MemoryMqTestSuite) SetupSuite() {
	m.mq = NewMq()
}

func (m *MemoryMqTestSuite) TestMq() {
	testcases := []struct {
		name      string
		consumers []mq.Consumer
		producers []mq.Producer
		wantValue []string
		input     []string
		err       error
	}{
		{
			name: "同一topic,单一消费者,生产者",
			consumers: func() []mq.Consumer {
				c, err := m.mq.Consumer("test_topic", "c1")
				require.NoError(m.T(), err)
				return []mq.Consumer{c}
			}(),
			producers: func() []mq.Producer {
				p, err := m.mq.Producer("test_topic")
				require.NoError(m.T(), err)
				return []mq.Producer{p}
			}(),
			wantValue: []string{"a", "b", "c", "d"},
			input:     []string{"a", "b", "c", "d"},
		},
	}
	for _, tc := range testcases {
		m.T().Run(tc.name, func(t *testing.T) {
			// 启动一批生产者生产数据
			var wg sync.WaitGroup
			for _, producer := range tc.producers {
				wg.Add(1)
				go func(p mq.Producer) {
					defer wg.Done()
					for _, msg := range tc.input {
						message := &mq.Message{Value: []byte(msg)}
						_, err := p.Produce(context.Background(), message)
						assert.NoError(m.T(), err)
					}
				}(producer)
			}
			// 启动消费者
			ansList := make([][]string, len(tc.consumers))
			for idx, comsumer := range tc.consumers {
				wg.Add(1)
				index := idx
				go func(c mq.Consumer, i int) {
					defer wg.Done()
					ans := make([]string, 0, len(tc.wantValue))
					msgCh, err := c.ConsumeMsgCh(context.Background())
					assert.NoError(m.T(), err)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					for {
						if len(ans) == len(tc.input) {
							ansList[i] = ans
							return
						}
						select {
						case msg := <-msgCh:
							ans = append(ans, string(msg.Value))
						case <-ctx.Done():
							return
						}
					}
				}(comsumer, index)
			}
			wg.Wait()
			for _, a := range ansList {
				assert.Equal(t, tc.wantValue, a)
			}

		})
	}
}

func TestMq(t *testing.T) {
	suite.Run(t, &MemoryMqTestSuite{})
}
