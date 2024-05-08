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
	"context"
	"testing"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopic_Close(t *testing.T) {
	t.Parallel()
	topic := newTopic("test_topic", 3)
	p1 := &Producer{
		t: topic,
	}
	p2 := &Producer{
		t: topic,
	}
	p3 := &Producer{
		t: topic,
	}
	err := topic.addProducer(p1)
	require.NoError(t, err)
	err = topic.addProducer(p2)
	require.NoError(t, err)
	err = topic.Close()
	require.NoError(t, err)
	require.Equal(t, true, topic.closed)
	err = topic.Close()
	require.NoError(t, err)
	require.Equal(t, true, topic.closed)
	err = topic.addProducer(p3)
	assert.Equal(t, errs.ErrMQIsClosed, err)
	_, err = p1.Produce(context.Background(), &mq.Message{
		Value: []byte("1"),
	})
	assert.Equal(t, errs.ErrProducerIsClosed, err)
}
