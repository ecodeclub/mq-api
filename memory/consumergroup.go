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
	"sync/atomic"
	"time"

	"github.com/ecodeclub/mq-api"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/pkg/errors"
)

var ErrReportOffsetFail = errors.New("非平衡状态，无法上报偏移量")

const (
	consumerCap      = 16
	defaultEventCap  = 16
	msgChannelLength = 1000

	// ExitGroup 退出信号
	ExitGroup = "exit_group"
	// ReportOffset 上报偏移量信号
	ReportOffset = "report_offset"
	// Rejoin 通知consumer重新加入消费组
	Rejoin = "rejoin"
	// RejoinAck 表示客户端收到重新加入消费组的指令并将offset进行上报
	RejoinAck = "rejoin_ack"
	Close     = "close"
	// PartitionNotify 下发分区情况事件
	PartitionNotify = "partition_notify"
	// PartitionNotifyAck 下发分区情况确认
	PartitionNotifyAck = "partition_notify_ack"

	StatusStable    = 1 // 稳定状态，可以正常的进行消费数据
	StatusBalancing = 2
)

// ConsumerGroup 表示消费组是并发安全的
type ConsumerGroup struct {
	name string
	// 存储消费者元数据，键为消费者的名称
	consumers syncx.Map[string, *ConsumerMetaData]
	// 消费者平衡器
	consumerPartitionBalancer ConsumerPartitionAssigner
	//  分区消费记录
	partitionRecords *syncx.Map[int, PartitionRecord]
	// 分区
	partitions []*Partition
	status     int32
	// 用于接受在重平衡阶段channel的返回数据
	balanceCh chan struct{}
}

type PartitionRecord struct {
	// 属于哪个分区
	Index int
	// 消费进度
	Cursor int
}
type ReportData struct {
	Records []PartitionRecord
	ErrChan chan error
}

type ConsumerMetaData struct {
	// 用于消费者上报数据，如退出或加入消费组，上报消费者消费分区的偏移量
	reportCh chan *Event
	// 用于消费组给消费者下发数据，如下发开始重平衡开始通知，告知消费者可以消费的channel
	receiveCh chan *Event
	// 消费者的名字
	name string
}

type Event struct {
	// 事件类型
	Type string
	// 事件所需要处理的数据
	Data any
}

func (c *ConsumerGroup) Handler(name string, event *Event) {
	switch event.Type {
	case ExitGroup:
		closeCh, _ := event.Data.(chan struct{})
		c.ExitGroup(name, closeCh)
	case ReportOffset:
		data, _ := event.Data.(ReportData)
		var err error
		err = c.ReportOffset(data.Records)
		data.ErrChan <- err
		log.Printf("消费者%s上报offset成功", name)
	case RejoinAck:
		// consumer响应重平衡信号返回的数据，返回的是当前所有分区的偏移量
		records, _ := event.Data.([]PartitionRecord)
		// 不管上报成不成功
		_ = c.ReportOffset(records)
		log.Printf("消费者%s成功接受到重平衡信号，并上报offset", name)
		c.balanceCh <- struct{}{}
	case PartitionNotifyAck:
		log.Printf("消费者%s 成功设置分区信息", name)
		c.balanceCh <- struct{}{}
	}
}

// ExitGroup 退出消费组
func (c *ConsumerGroup) ExitGroup(name string, closeCh chan struct{}) {
	// 把自己从消费组内摘除
	for {
		if !atomic.CompareAndSwapInt32(&c.status, StatusStable, StatusBalancing) {
			continue
		}
		log.Printf("消费者 %s 准备退出消费组", name)
		c.consumers.Delete(name)
		c.reBalance()
		log.Printf("给消费者 %s 发送退出确认信号", name)
		close(closeCh)
		log.Printf("消费者 %s 成功退出消费组", name)
		atomic.CompareAndSwapInt32(&c.status, StatusBalancing, StatusStable)
		return
	}
}

// ReportOffset 上报偏移量
func (c *ConsumerGroup) ReportOffset(records []PartitionRecord) error {
	if atomic.LoadInt32(&c.status) != StatusStable {
		return ErrReportOffsetFail
	}
	for _, record := range records {
		c.partitionRecords.Store(record.Index, record)
	}
	return nil
}

func (c *ConsumerGroup) Close() {
	c.consumers.Range(func(key string, value *ConsumerMetaData) bool {
		value.receiveCh <- &Event{
			Type: Close,
		}
		return true
	})
	// 等待一秒退出完成
	time.Sleep(1 * time.Second)
}

// reBalance 单独使用该方法是并发不安全的
func (c *ConsumerGroup) reBalance() {
	log.Println("开始重平衡")
	// 通知每一个消费者进行偏移量的上报
	length := 0
	consumers := make([]string, 0, consumerCap)
	log.Println("开始给每个消费者，重平衡信号")
	c.consumers.Range(func(key string, value *ConsumerMetaData) bool {
		log.Printf("开始通知消费者%s", key)
		value.receiveCh <- &Event{
			Type: Rejoin,
		}
		consumers = append(consumers, key)
		length++
		log.Printf("通知消费者%s成功", key)
		return true
	})
	number := 0
	// 等待所有消费者都接收到信号，并上报自己offset
	for length > 0 {
		select {
		case <-c.balanceCh:
			number++
			if number == length {
				// 接收到所有信号
				log.Println("所有消费者已经接受到重平衡请求，并上报了消费进度")
				consumerMap := c.consumerPartitionBalancer.AssignPartition(consumers, len(c.partitions))
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
							Type: PartitionNotify,
							Data: records,
						}
						// 等待消费者接收到并保存
						<-c.balanceCh

					}
				}
				log.Println("重平衡结束")
				return
			}
		default:

		}
	}
	log.Println("重平衡结束")
}

// JoinGroup 加入消费组
func (c *ConsumerGroup) JoinGroup() *Consumer {
	for {
		if !atomic.CompareAndSwapInt32(&c.status, StatusStable, StatusBalancing) {
			continue
		}
		var length int
		c.consumers.Range(func(key string, value *ConsumerMetaData) bool {
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
		c.consumers.Store(name, &ConsumerMetaData{
			reportCh:  reportCh,
			receiveCh: receiveCh,
			name:      name,
		})
		go c.HandleConsumerSignals(name, reportCh)
		go consumer.Run()
		log.Printf("新建消费者 %s", name)
		// 重平衡分配分区
		c.reBalance()
		atomic.CompareAndSwapInt32(&c.status, StatusBalancing, StatusStable)
		return consumer
	}
}

// HandleConsumerSignals 处理消费者上报的事件
func (c *ConsumerGroup) HandleConsumerSignals(name string, reportCh chan *Event) {
	for event := range reportCh {
		c.Handler(name, event)
		if event.Type == ExitGroup {
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
