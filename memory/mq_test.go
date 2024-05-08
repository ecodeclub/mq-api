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
