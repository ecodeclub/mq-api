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
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ecodeclub/mq-api"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/pkg/errors"
)

var (
	ErrReportOffsetFail    = errors.New("非平衡状态，无法上报偏移量")
	ErrConsumerGroupClosed = errors.New("消费组已经关闭")
)

const (
	consumerCap      = 16
	defaultEventCap  = 16
	msgChannelLength = 1000
	defaultSleepTime = 100 * time.Millisecond

	// ExitGroupEvent consumer=>consumer_group 表示消费者退出消费组的事件
	ExitGroupEvent = "exit_group"
	// ReportOffsetEvent consumer=>consumer_group  表示消费者向消费组上报消费进度事件
	ReportOffsetEvent = "report_offset"
	// RejoinEvent consumer_group=>consumer  表示消费组通知消费者重新加入消费组
	RejoinEvent = "rejoin"
	// RejoinAckEvent  consumer=>consumer_group  表示消费者收到重新加入消费组的指令并将offset进行上报
	RejoinAckEvent = "rejoin_ack"
	// CloseEvent consumer_group=>consumer 表示消费组关闭所有消费者，向所有消费者发出关闭事件
	CloseEvent = "close"
	// PartitionNotifyEvent consumer_group=>consumer 表示消费组向消费者下发分区情况
	PartitionNotifyEvent = "partition_notify"
	// PartitionNotifyAckEvent consumer=>consumer_group  表示消费者对消费组下发分区情况事件的确认
	PartitionNotifyAckEvent = "partition_notify_ack"

	StatusStable    = 1 // 稳定状态，可以正常的进行消费数据
	StatusBalancing = 2
	// 消费组关闭
	StatusStop = 3
	// 一个消费者正在退出消费组
	StatusStopping = 4
)

// ConsumerGroup 表示消费组是并发安全的
type ConsumerGroup struct {
	name string
	// 存储消费者元数据，键为消费者的名称
	consumers syncx.Map[string, *Consumer]
	// 消费者平衡器
	consumerPartitionAssigner ConsumerPartitionAssigner
	//  分区消费记录
	partitionRecords *syncx.Map[int, PartitionRecord]
	// 分区
	partitions []*Partition
	status     int32
	balanceCh  chan struct{}
	once       sync.Once
}

type PartitionRecord struct {
	// 属于哪个分区
	Index int
	// 消费进度
	Offset int
}
type ReportData struct {
	Records []PartitionRecord
	ErrChan chan error
}

type Event struct {
	// 事件类型
	Type string
	// 事件所需要处理的数据
	Data any
}

func (c *ConsumerGroup) eventHandler(name string, event *Event) {
	switch event.Type {
	case ExitGroupEvent:
		closeCh, _ := event.Data.(chan struct{})
		c.exitGroup(name, closeCh)
	case ReportOffsetEvent:
		data, _ := event.Data.(ReportData)
		var err error
		err = c.reportOffset(data.Records)
		data.ErrChan <- err
		log.Printf("消费者%s上报offset成功", name)
	case RejoinAckEvent:
		// consumer响应重平衡信号返回的数据，返回的是当前所有分区的偏移量
		records, _ := event.Data.([]PartitionRecord)
		// 不管上报成不成功
		_ = c.reportOffset(records)
		log.Printf("消费者%s成功接受到重平衡信号，并上报offset", name)
		c.balanceCh <- struct{}{}
	case PartitionNotifyAckEvent:
		log.Printf("消费者%s 成功设置分区信息", name)
		c.balanceCh <- struct{}{}
	}
}

// ExitGroupEvent 退出消费组
func (c *ConsumerGroup) exitGroup(name string, closeCh chan struct{}) {
	// 把自己从消费组内摘除
	for {
		if !atomic.CompareAndSwapInt32(&c.status, StatusStable, StatusBalancing) &&
			!atomic.CompareAndSwapInt32(&c.status, StatusStop, StatusStopping) {
			time.Sleep(defaultSleepTime)
			continue
		}
		log.Printf("消费者 %s 准备退出消费组", name)
		c.consumers.Delete(name)
		c.reBalance()
		log.Printf("给消费者 %s 发送退出确认信号", name)
		close(closeCh)
		log.Printf("消费者 %s 成功退出消费组", name)
		if !atomic.CompareAndSwapInt32(&c.status, StatusBalancing, StatusStable) {
			atomic.CompareAndSwapInt32(&c.status, StatusStopping, StatusStop)
		}
		return
	}
}

// ReportOffsetEvent 上报偏移量
func (c *ConsumerGroup) reportOffset(records []PartitionRecord) error {
	status := atomic.LoadInt32(&c.status)
	if status != StatusStable && status != StatusStop {
		return ErrReportOffsetFail
	}
	for _, record := range records {
		c.partitionRecords.Store(record.Index, record)
	}
	return nil
}

func (c *ConsumerGroup) Close() {
	c.once.Do(func() {
		for {
			if !atomic.CompareAndSwapInt32(&c.status, StatusStable, StatusStop) {
				time.Sleep(defaultSleepTime)
				continue
			}
			c.close()
			return
		}
	})
}

func (c *ConsumerGroup) close() {
	c.consumers.Range(func(key string, value *Consumer) bool {
		ch := make(chan struct{})
		value.receiveCh <- &Event{
			Type: CloseEvent,
			Data: ch,
		}
		<-ch
		return true
	})
}

// reBalance 单独使用该方法是并发不安全的
func (c *ConsumerGroup) reBalance() {
	log.Println("开始重平衡")
	// 通知每一个消费者进行偏移量的上报
	length := 0
	consumers := make([]string, 0, consumerCap)
	log.Println("开始给每个消费者，重平衡信号")
	c.consumers.Range(func(key string, value *Consumer) bool {
		log.Printf("开始通知消费者%s", key)
		value.receiveCh <- &Event{
			Type: RejoinEvent,
		}
		consumers = append(consumers, key)
		length++
		log.Printf("通知消费者%s成功", key)
		return true
	})
	number := 0
	log.Println("xxxxxxxxxx长度", length)
	// 等待所有消费者都接收到信号，并上报自己offset
	for length > 0 {
		<-c.balanceCh
		number++
		if number != length {
			log.Println("xxxxxxxxxx number", number)
			continue
		}
		// 接收到所有信号
		log.Println("所有消费者已经接受到重平衡请求，并上报了消费进度")
		consumerMap := c.consumerPartitionAssigner.AssignPartition(consumers, len(c.partitions))
		// 通知所有消费者分配
		log.Println("开始分配分区")
		for consumerName, partitions := range consumerMap {
			// 查找消费者所属的channel
			log.Printf("消费者 %s 消费 %v 分区", consumerName, partitions)
			consumer, ok := c.consumers.Load(consumerName)
			if ok {
				// 往每个消费者的receive_channel发送partition的信息
				records := make([]PartitionRecord, 0, len(partitions))
				for _, p := range partitions {
					record, ok := c.partitionRecords.Load(p)
					if ok {
						records = append(records, record)
					}
				}
				consumer.receiveCh <- &Event{
					Type: PartitionNotifyEvent,
					Data: records,
				}
				// 等待消费者接收到并保存
				<-c.balanceCh

			}
		}
		log.Println("重平衡结束")
		return
	}
}

// JoinGroup 加入消费组
func (c *ConsumerGroup) JoinGroup() (*Consumer, error) {
	for {

		if atomic.LoadInt32(&c.status) > StatusBalancing {
			return nil, ErrConsumerGroupClosed
		}
		if !atomic.CompareAndSwapInt32(&c.status, StatusStable, StatusBalancing) {
			time.Sleep(defaultSleepTime)
			continue
		}

		var length int
		c.consumers.Range(func(key string, value *Consumer) bool {
			length++
			return true
		})
		name := fmt.Sprintf("%s_%d", c.name, length)
		reportCh := make(chan *Event, defaultEventCap)
		receiveCh := make(chan *Event, defaultEventCap)
		consumer := &Consumer{
			partitions:       c.partitions,
			receiveCh:        receiveCh,
			reportCh:         reportCh,
			name:             name,
			msgCh:            make(chan *mq.Message, msgChannelLength),
			partitionRecords: []PartitionRecord{},
			closeCh:          make(chan struct{}),
		}
		c.consumers.Store(name, consumer)
		go c.consumerEventsHandler(name, reportCh)
		go consumer.eventLoop()
		log.Printf("新建消费者 %s", name)
		// 重平衡分配分区
		c.reBalance()
		atomic.CompareAndSwapInt32(&c.status, StatusBalancing, StatusStable)
		return consumer, nil
	}
}

// consumerEventsHandler 处理消费者上报的事件
func (c *ConsumerGroup) consumerEventsHandler(name string, reportCh chan *Event) {
	for event := range reportCh {
		c.eventHandler(name, event)
		if event.Type == ExitGroupEvent {
			close(reportCh)
			return
		}
	}
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
