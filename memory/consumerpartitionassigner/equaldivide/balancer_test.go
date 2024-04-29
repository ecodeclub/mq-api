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

package equaldivide

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBalancer_AssignPartition(t *testing.T) {
	t.Parallel()
	balancer := NewAssigner()
	testcases := []struct {
		name       string
		consumers  []string
		partition  int
		wantAnswer map[string][]int
	}{
		{
			name:      "分区数超过consumer个数",
			consumers: []string{"c1", "c2", "c3", "c4"},
			partition: 5,
			wantAnswer: map[string][]int{
				"c1": {0, 1},
				"c2": {2},
				"c3": {3},
				"c4": {4},
			},
		},
		{
			name:      "分区数小于consumer个数",
			consumers: []string{"c1", "c2", "c3", "c4"},
			partition: 3,
			wantAnswer: map[string][]int{
				"c1": {0},
				"c2": {1},
				"c3": {2},
				"c4": {},
			},
		},
		{
			name:      "分区数等于consumer个数",
			consumers: []string{"c1", "c2", "c3"},
			partition: 3,
			wantAnswer: map[string][]int{
				"c1": {0},
				"c2": {1},
				"c3": {2},
			},
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actualVal := balancer.AssignPartition(tc.consumers, tc.partition)
			assert.Equal(t, tc.wantAnswer, actualVal)
		})
	}
}
