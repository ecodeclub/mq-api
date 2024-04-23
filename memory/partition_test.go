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

	"github.com/ecodeclub/mq-api"
	"github.com/stretchr/testify/assert"
)

func Test_Partition(t *testing.T) {
	t.Parallel()
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
