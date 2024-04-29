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
	"sync"

	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/mq-api"
)

// Partition 表示分区 是并发安全的
const (
	defaultPartitionCap = 64
)

type Partition struct {
	locker sync.RWMutex
	data   *list.ArrayList[*mq.Message]
}

func NewPartition() *Partition {
	return &Partition{
		data: list.NewArrayList[*mq.Message](defaultPartitionCap),
	}
}

func (p *Partition) append(msg *mq.Message) {
	p.locker.Lock()
	defer p.locker.Unlock()
	msg.Offset = int64(p.data.Len())
	_ = p.data.Append(msg)
}

func (p *Partition) getBatch(offset, limit int) []*mq.Message {
	p.locker.RLock()
	defer p.locker.RUnlock()
	wantLen := offset + limit
	length := min(wantLen, p.data.Len())
	res := p.data.AsSlice()[offset:length]
	return res
}
