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

type Assigner struct{}

func (b *Assigner) AssignPartition(consumers []string, partitions int) map[string][]int {
	result := make(map[string][]int)
	consumerCount := len(consumers)
	partitionPerConsumer := partitions / consumerCount
	remainingPartitions := partitions % consumerCount

	// 初始化每个 consumer 对应的 partitions
	for _, consumer := range consumers {
		result[consumer] = make([]int, 0)
	}
	// 平均分配 partitions
	partitionIndex := 0
	for i := 0; i < consumerCount; i++ {
		consumer := consumers[i]
		numPartitions := partitionPerConsumer
		// 如果还有剩余的 partitions，则将其分配给当前 consumer
		if remainingPartitions > 0 {
			numPartitions++
			remainingPartitions--
		}
		// 分配 partitions
		for j := 0; j < numPartitions; j++ {
			result[consumer] = append(result[consumer], partitionIndex)
			partitionIndex++
		}
	}
	return result
}

func NewAssigner() *Assigner {
	return &Assigner{}
}
