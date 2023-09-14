package poll

import "sync"

type Getter struct {
	cursor    int64
	partition int64
	locker    sync.Locker
}

func (g *Getter) Get() int64 {
	g.locker.Lock()
	defer g.locker.Unlock()
	g.cursor++
	g.cursor = g.cursor % g.partition
	return g.cursor
}

func NewGetter(partition int64) *Getter {
	return &Getter{
		cursor:    0,
		partition: partition,
		locker:    &sync.RWMutex{},
	}
}
