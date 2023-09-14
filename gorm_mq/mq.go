package gorm_mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/gorm_mq/balancer/equal_divide"
	"github.com/ecodeclub/mq-api/gorm_mq/domain"
	"github.com/ecodeclub/mq-api/gorm_mq/getter/poll"
	"gorm.io/gorm"
	"log"
	"sync"
	"time"
)

type Mq struct {
	Db               *gorm.DB
	producerGetter   ProducerGetter
	consumerBalancer ConsumerBalancer
	topics           syncx.Map[string, *Topic]
}

type Topic struct {
	Name           string
	partitionNum   int
	producerGetter ProducerGetter
	lock           sync.RWMutex
	partitionList  []string
	Consumers      []*MqConsumer
	producerCh     chan *mq.Message
	MsgCh          []chan *mq.Message
}

func (m *Mq) Topic(name string, partition int) error {
	partitionList := make([]string, 0, partition)
	for i := 0; i < partition; i++ {
		tableName, err := m.genPartition(name, i)
		if err != nil {
			return err
		}
		partitionList = append(partitionList, tableName)
	}

	producerCh := make(chan *mq.Message, 1000)
	t := &Topic{
		Name:           name,
		partitionList:  partitionList,
		producerCh:     producerCh,
		partitionNum:   partition,
		producerGetter: poll.NewGetter(int64(partition)),
	}
	m.topics.Store(name, t)
	go func() {
		for {
			select {
			case msg := <-producerCh:
				for i := 0; i < len(t.MsgCh); i++ {
					t.MsgCh[i] <- msg
				}
			}
		}
	}()
	return nil
}

func (m *Mq) genPartition(name string, partition int) (tableName string, err error) {
	tableName = fmt.Sprintf("%s_%d", name, partition)
	err = m.Db.Table(tableName).AutoMigrate(&domain.Partition{})
	return tableName, err
}

func (m *Mq) Producer(topic string) (mq.Producer, error) {
	tp, ok := m.topics.Load(topic)
	if !ok {
		return nil, errors.New("topic 不存在")
	}
	return &MqProducer{
		Topic:  tp,
		DB:     m.Db,
		getter: tp.producerGetter,
	}, nil
}

func (m *Mq) Consumer(topic string) (mq.Consumer, error) {
	tp, ok := m.topics.Load(topic)
	if !ok {
		return nil, errors.New("topic 不存在")
	}
	msgCh := make(chan *mq.Message, 100)
	tp.MsgCh = append(tp.MsgCh, msgCh)
	mqConsumer := &MqConsumer{
		topic:      tp,
		db:         m.Db,
		partitions: make([]int, 0, 32),
		msgCh:      msgCh,
	}
	// 重新分配consumer对应的分区
	tp.lock.Lock()
	res := m.consumerBalancer.Balance(tp.partitionNum, len(tp.Consumers)+1)
	for i := 0; i < len(tp.Consumers); i++ {
		tp.Consumers[i].partitions = res[i]
	}
	mqConsumer.partitions = res[len(tp.Consumers)]
	tp.Consumers = append(tp.Consumers, mqConsumer)
	tp.lock.Unlock()
	go func() {
		timer := time.NewTicker(2 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
				msgs, err := mqConsumer.getMsgFromDB(ctx)
				if err != nil {
					cancel()
					log.Println(err)
					continue
				}
				cancel()
				for _, msg := range msgs {
					tp.producerCh <- msg
				}
			}
		}
	}()
	return mqConsumer, nil
}

func NewMq(Db *gorm.DB) (mq.MQ, error) {
	err := Db.AutoMigrate(&domain.Cursors{})
	if err != nil {
		return nil, err
	}
	return &Mq{
		Db:               Db,
		topics:           syncx.Map[string, *Topic]{},
		consumerBalancer: equal_divide.NewBalancer(),
	}, nil
}

type MqProducer struct {
	*Topic
	DB     *gorm.DB
	getter ProducerGetter
}

func (m2 *MqProducer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	tableName := m2.Topic.partitionList[m2.getter.Get()]
	newMsg, err := NewMessage(m)
	if err != nil {
		return nil, err
	}
	err = m2.DB.Table(tableName).WithContext(ctx).Create(newMsg).Error
	return nil, err
}

func NewMessage(m *mq.Message) (*domain.Partition, error) {
	val, err := json.Marshal(m.Header)
	if err != nil {
		return nil, err
	}
	return &domain.Partition{
		Value:  string(m.Value),
		Key:    string(m.Key),
		Topic:  m.Topic,
		Header: string(val),
	}, nil
}

type MqConsumer struct {
	topic      *Topic
	db         *gorm.DB
	partitions []int
	msgCh      chan *mq.Message
}

func (m *MqConsumer) Consume(ctx context.Context) (*mq.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-m.msgCh:
		return msg, nil
	}
}

func (m *MqConsumer) ConsumeMsgCh(ctx context.Context) (<-chan *mq.Message, error) {
	return m.msgCh, nil
}

func (m *MqConsumer) getMsgFromDB(ctx context.Context) ([]*mq.Message, error) {
	ans := make([]*mq.Message, 0, 64)
	for _, p := range m.partitions {
		// 获取表的游标
		cursor := &domain.Cursors{}
		err := m.db.WithContext(ctx).Table(cursor.TableName()).Where("`table` = ?", fmt.Sprintf("%s_%d", m.topic.Name, p)).First(&cursor).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				err = m.db.WithContext(ctx).Table(cursor.TableName()).Create(&domain.Cursors{
					Table:  fmt.Sprintf("%s_%d", m.topic.Name, p),
					Cursor: 0,
				}).Error
				if err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		// 从表内获取数据
		var vals []*domain.Partition
		err = m.db.Table(cursor.Table).Where("id > ?", cursor.Cursor).Find(&vals).Error
		if err != nil {
			return nil, err
		}
		// 将最后一个游标写入
		if len(vals) > 0 {
			cursor.Cursor = int64(vals[len(vals)-1].ID)
		}
		err = m.db.WithContext(ctx).Table(cursor.TableName()).Updates(cursor).Error
		if err != nil {
			return nil, err
		}
		// 转化成msg
		for _, val := range vals {
			msg, err := convertToMsg(val)
			if err != nil {
				return nil, err
			}
			ans = append(ans, msg)
		}
	}
	return ans, nil
}

func convertToMsg(partition *domain.Partition) (*mq.Message, error) {
	header := mq.Header{}
	if partition.Header != "" {
		err := json.Unmarshal([]byte(partition.Header), &header)
		if err != nil {
			return nil, err
		}
	}
	return &mq.Message{
		Value:  []byte(partition.Value),
		Key:    []byte(partition.Key),
		Header: header,
		Topic:  partition.Topic,
	}, nil
}
