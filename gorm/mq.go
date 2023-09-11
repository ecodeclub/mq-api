package gorm

import (
	"fmt"
	"github.com/ecodeclub/ekit/syncx"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/gorm/domain"
	"gorm.io/gorm"
)

type Mq struct {
	Dd     *gorm.DB
	topics syncx.Map[string, *Topic]
}

type Topic struct {
	Name          string
	partitionList []string
}

type Partition struct {
	Name string `gorm:"_"`
	domain.MqMessage
}

func (p Partition) TableName() string {
	return p.Name
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
	t := &Topic{
		Name:          name,
		partitionList: partitionList,
	}
	m.topics.Store(name, t)
	return nil
}

func (m *Mq) genPartition(name string, partition int) (tableName string, err error) {
	tableName = fmt.Sprintf("%s_%d", name, partition)
	err = m.Dd.AutoMigrate(&Partition{
		Name: tableName,
	})
	return
}

func (m *Mq) Producer(topic string) (mq.Producer, error) {
	return nil, nil
}

func (m *Mq) Consumer(topic string) (mq.Consumer, error) {
	return nil, nil
}

func NewMq(Db *gorm.DB) mq.MQ {
	return nil
}
