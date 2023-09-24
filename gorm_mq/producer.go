package gorm_mq

import (
	"context"
	"encoding/json"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/gorm_mq/domain"
	"gorm.io/gorm"
)

type MqProducer struct {
	*Topic
	DB     *gorm.DB
	getter ProducerGetter
}

func (m2 *MqProducer) Produce(ctx context.Context, m *mq.Message) (*mq.ProducerResult, error) {
	tableName := m2.Topic.partitionList[m2.getter.Get(string(m.Key))]
	newMsg, err := NewMessage(m)
	if err != nil {
		return nil, err
	}
	newMsg.Topic = m2.Topic.Name
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
