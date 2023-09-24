package poll

import (
	"hash/fnv"
)

type Getter struct {
	Partition int
}

// 暂时使用hash，保证同一个key的值，在同一个分区。
func (g *Getter) Get(key string) int64 {
	return hashString(key, g.Partition)
}

func hashString(s string, numBuckets int) int64 {
	h := fnv.New32a()
	h.Write([]byte(s))
	hash := h.Sum32()
	return int64(hash % uint32(numBuckets))
}
