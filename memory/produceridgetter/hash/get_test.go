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

package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetter(t *testing.T) {
	t.Parallel()
	// 测试两个相同的key返回的partition是同一个
	getter := Getter{
		3,
	}
	partition1 := getter.PartitionID("msg1")
	partition2 := getter.PartitionID("msg1")
	assert.Equal(t, partition1, partition2)
}
