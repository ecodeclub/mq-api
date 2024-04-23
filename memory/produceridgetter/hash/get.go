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

import "hash/fnv"

type Getter struct {
	Partitions int
}

// PartitionID 暂时使用hash，保证同一个key的值，在同一个分区。
func (g *Getter) PartitionID(key string) int64 {
	return hashString(key, g.Partitions)
}

func hashString(s string, numBuckets int) int64 {
	h := fnv.New32a()
	h.Write([]byte(s))
	hash := h.Sum32()
	return int64(hash % uint32(numBuckets))
}
