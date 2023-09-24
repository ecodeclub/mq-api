package gorm_mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/gorm_mq/domain"
	"github.com/ecodeclub/mq-api/mqerr"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sync"
	"time"
)

type MqConsumer struct {
	topic      *Topic
	db         *gorm.DB
	partitions []int
	msgCh      chan *mq.Message
	name       string
	groupId    string
	lock       sync.RWMutex
	// 每次至多消费多少
	limit int
	// 抢占超时时间
	timeout time.Duration
	// 续约时间
	interval time.Duration
	cursor   int64
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
		// 抢占获取表的游标,如果当前
		tableName := fmt.Sprintf("%s_%d", m.topic.Name, p)
		cursor, err := m.occupyCursor(tableName, m.groupId)
		if err != nil {
			continue
		}
		// 从表内获取数据，并进行自动续约
		ch := make(chan *msgRes, 1)
		go m.getMsg(ctx, ch, tableName, cursor)
		vals, err := m.autoRefresh(ctx, ch, tableName)
		if err != nil {
			continue
		}
		// 释放并更新游标
		if len(vals) != 0 {
			cursor = int64(vals[len(vals)-1].ID)
		}
		err = m.releaseCursor(tableName, m.groupId, cursor)
		if err != nil {
			continue
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

// 从表中获取数据
func (m *MqConsumer) getMsg(ctx context.Context, ch chan *msgRes, tableName string, cursor int64) {
	var vals []*domain.Partition
	err := m.db.WithContext(ctx).Table(tableName).Where("id > ?", cursor).Order("id ASC").Limit(m.limit).Find(&vals).Error
	if err != nil {
		ch <- &msgRes{
			err: err,
		}
		return
	}
	ch <- &msgRes{
		msgs: vals,
	}
	return

}

// 自动续约
func (m *MqConsumer) autoRefresh(ctx context.Context, ch chan *msgRes, tableName string) ([]*domain.Partition, error) {
	ticker := time.NewTicker(m.interval)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			err := m.refresh(ctx, tableName, m.groupId)
			if err != nil {
				return nil, err
			}
		case msgs := <-ch:
			if msgs.err != nil {
				return nil, msgs.err
			}
			return msgs.msgs, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

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

// 续约刷新
func (m *MqConsumer) refresh(ctx context.Context, tableName string, id string) error {
	// 判断当前的占有者是否为 自己  如果是就更新，如果不是就返回报错
	db := m.db
	res := db.WithContext(ctx).Model(&domain.Cursors{}).Where("`table` = ? AND consumer_group = ? AND occupant = ?", tableName, id, m.name).
		Update("updated_at", time.Now())
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return mqerr.ErrOccupierNotFound
	}
	return nil
}

// 抢占游标
func (m *MqConsumer) occupyCursor(tableName string, id string) (int64, error) {
	// 如果不存在该记录就写入，如果存在，判断occupant该字段是否为空,为空将name更新到occupant字段，不为空，判断更新时间和现在时间是否相差十秒（续约时间一定要小于这个判断时间）。
	// 如果是还是将name更新到occupant字段
	// 开始数据库事务
	tx := m.db.Begin()
	// 查找记录并锁定行
	existingRecord := &domain.Cursors{
		Table:         tableName,
		ConsumerGroup: id,
		Occupant:      m.name,
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("`table` = ? AND `consumer_group` = ?", tableName, id).First(&existingRecord).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 记录不存在，插入新记录
			existingRecord.Occupant = m.name
			if err := tx.Create(&existingRecord).Error; err != nil {
				tx.Rollback()
				return 0, err
			}
		} else {
			tx.Rollback()
			return 0, err
		}
	} else {
		// 记录存在，进行更新操作
		if existingRecord.Occupant == "" {
			if err := tx.Model(&existingRecord).Update("occupant", m.name).Error; err != nil {
				tx.Rollback()
				return 0, err
			}
		} else {
			// 记录不为空，检查时间差是否小于超时时间
			currentTime := time.Now()
			if currentTime.Sub(existingRecord.UpdatedAt) > m.timeout {
				if err := tx.Model(&existingRecord).Update("occupant", m.name).Error; err != nil {
					tx.Rollback()
					return 0, err
				}
			} else {
				tx.Commit()
				return 0, mqerr.ErrOccupied
			}
		}
	}
	// 提交事务
	return existingRecord.Cursor, tx.Commit().Error
}

// 释放并更新游标
func (m *MqConsumer) releaseCursor(tableName string, id string, cursor int64) error {
	// 如果该记录存在且游标的占有者为自己就更新游标，并将占有者改成""
	// 开启数据库事务
	tx := m.db.Begin()
	// 查询记录并加行级锁
	var record domain.Cursors
	result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("`table` = ? AND consumer_group = ? AND occupant = ?", tableName, id, m.name).
		First(&record)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			tx.Commit()
			return mqerr.ErrOccupied
		} else {
			tx.Rollback()
			return result.Error
		}
	} else {
		// 记录存在，更新 occupant 和 cursor
		record.Occupant = ""
		record.Cursor = cursor
		if err := tx.Save(&record).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit().Error

}

type msgRes struct {
	msgs []*domain.Partition
	err  error
}

func (m *MqConsumer) getCursor() int64 {
	val := m.cursor
	newVal := (m.cursor + 1) % int64(len(m.partitions))
	m.cursor = newVal
	return val
}
