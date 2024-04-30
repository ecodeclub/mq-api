package memory

import (
	"testing"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMQ(t *testing.T) {
	t.Parallel()
	// 测试调用consumer 和 producer 如果topic不存在就新建
	testmq := &MQ{
		topics: syncx.Map[string, *Topic]{},
	}
	_, err := testmq.Consumer("test_topic", "group1")
	require.NoError(t, err)
	_, ok := testmq.topics.Load("test_topic")
	assert.Equal(t, ok, true)
	_, err = testmq.Producer("test_topic1")
	require.NoError(t, err)
	_, ok = testmq.topics.Load("test_topic1")
	assert.Equal(t, ok, true)
}
