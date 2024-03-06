package memory

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/mqerr"
)

const (
	interval                  = 1 * time.Second
	defaultMessageChannelSize = 1000
	// 每个分区取数据的上限
	limit = 25
)

var ErrConsumerClose = errors.New("消费者已关闭")

type Consumer struct {
	locker sync.RWMutex
	name   string
	closed bool
	// 用于存放分区号，每个元素就是一个分区号
	partitions       []*Partition
	partitionRecords []PartitionRecord
	closeCh          chan struct{}
	msgCh            chan *mq.Message
	once             sync.Once
	reportCh         chan *Event
	receiveCh        chan *Event
}

func (c *Consumer) Consume(ctx context.Context) (*mq.Message, error) {
	if c.isClosed() {
		return nil, mqerr.ErrConsumerIsClosed
	}
	select {
	case val, ok := <-c.msgCh:
		if !ok {
			return nil, mqerr.ErrConsumerIsClosed
		}
		return val, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// 启动Consume
func (c *Consumer) Run() {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("消费者 %s 开始消费数据", c.name)
			for idx, record := range c.partitionRecords {
				msgs := c.partitions[record.Index].consumerMsg(record.Cursor, limit)
				for _, msg := range msgs {
					log.Printf("消费者 %s 消费数据 %v", c.name, msg)
					c.msgCh <- msg
				}
				record.Cursor += len(msgs)
				errCh := make(chan error, 1)
				c.reportCh <- &Event{
					Type: ReportOffset,
					Data: ReportData{
						Records: []PartitionRecord{record},
						ErrChan: errCh,
					},
				}
				log.Printf("获取是否消费成功", c.name)
				err := <-errCh
				if err != nil {
					log.Printf("上报偏移量失败：%v", err)
					break
				}
				close(errCh)
				c.partitionRecords[idx] = record
			}
			log.Printf("消费者 %s 结束消费数据", c.name)
		case event, ok := <-c.receiveCh:
			log.Println(ok, "xxxxxxxxxxxxooooooooooo", c.name)
			if !ok {
				return
			}
			log.Println(event.Type, "xxxxxxxxxxxx", c.name)
			// 处理各种事件
			c.Handle(event)
		}
	}
}

func (c *Consumer) Handle(event *Event) {
	switch event.Type {
	// 服务端发起的重平衡事件
	case Rejoin:
		// 消费者上报消费进度
		log.Printf("消费者 %s开始上报消费进度", c.name)
		c.reportCh <- &Event{
			Type: RejoinAck,
			Data: c.partitionRecords,
		}
		// 设置消费进度
		partitionInfo := <-c.receiveCh
		log.Printf("消费者 %s接收到分区信息 %v", c.name, partitionInfo)
		c.partitionRecords = partitionInfo.Data.([]PartitionRecord)
		// 返回设置完成的信号
		c.reportCh <- &Event{
			Type: PartitionNotifyAck,
		}
	case Close:
		c.Close()
	}
}

func (c *Consumer) ConsumeChan(ctx context.Context) (<-chan *mq.Message, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.isClosed() {
		return nil, mqerr.ErrConsumerIsClosed
	}
	return c.msgCh, nil
}

func (c *Consumer) Close() error {
	c.locker.Lock()
	defer c.locker.Unlock()
	c.once.Do(func() {
		c.closed = true
		c.reportCh <- &Event{
			Type: ExitGroup,
			Data: c.closeCh,
		}
		log.Printf("消费者 %s 准备关闭", c.name)
		// 等待服务端退出完成
		<-c.closeCh
		// 关闭资源
		close(c.receiveCh)
		close(c.msgCh)
		log.Printf("消费者 %s 关闭成功", c.name)
	})

	return nil
}

func (c *Consumer) isClosed() bool {
	c.locker.RLock()
	defer c.locker.RUnlock()
	return c.closed
}
