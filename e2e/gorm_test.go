//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/gorm_mq"
	"github.com/ecodeclub/mq-api/gorm_mq/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sync"
	"testing"
	"time"
)

type GormMQSuite struct {
	suite.Suite
	dsn string
	db  *gorm.DB
}

func (g *GormMQSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open(g.dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	require.NoError(g.T(), err)
	g.db = db
}
func (g *GormMQSuite) TearDownTest() {
	sqlDB, err := g.db.DB()
	require.NoError(g.T(), err)
	_, err = sqlDB.Exec("DROP DATABASE IF EXISTS `test`")
	require.NoError(g.T(), err)
	_, err = sqlDB.Exec("CREATE DATABASE  `test`")
	require.NoError(g.T(), err)
}

func (g *GormMQSuite) TestTopic() {
	testcases := []struct {
		name    string
		topic   string
		input   int
		wantVal []string
	}{
		{
			name:    "建立含有4个分区的topic",
			topic:   "test_topic",
			input:   4,
			wantVal: []string{"test_topic_0", "test_topic_1", "test_topic_2", "test_topic_3"},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			mq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = mq.Topic(tc.topic, 4)
			require.NoError(t, err)
			tables, err := g.getTables(tc.topic)
			require.NoError(t, err)
			assert.Equal(t, tc.wantVal, tables)
		})
	}

}

func (g *GormMQSuite) TestConsumer() {
	testcases := []struct {
		name       string
		topic      string
		input      []*mq.Message
		partitions int64
		consumers  func(mq mq.MQ) []mq.Consumer
		// 处理消息
		consumerFunc func(c mq.Consumer) []*mq.Message
		wantVal      []*mq.Message
	}{
		{
			name:       "一个消费组内多个消费者",
			topic:      "test_topic",
			partitions: 2,
			input: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c12, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c13, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(g.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic",
				},
			},
		},
		{
			name:       "多个消费组，多个消费者",
			topic:      "test_topic",
			partitions: 3,
			input: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
				},
			},
			consumers: func(mqm mq.MQ) []mq.Consumer {
				c11, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c12, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c13, err := mqm.Consumer("test_topic", "c1")
				require.NoError(g.T(), err)
				c21, err := mqm.Consumer("test_topic", "c2")
				require.NoError(g.T(), err)
				c22, err := mqm.Consumer("test_topic", "c2")
				require.NoError(g.T(), err)
				c23, err := mqm.Consumer("test_topic", "c2")
				require.NoError(g.T(), err)
				return []mq.Consumer{
					c11,
					c12,
					c13,
					c21,
					c22,
					c23,
				}
			},
			consumerFunc: func(c mq.Consumer) []*mq.Message {
				msgCh, err := c.ConsumeMsgCh(context.Background())
				require.NoError(g.T(), err)
				msgs := make([]*mq.Message, 0, 32)
				for val := range msgCh {
					msgs = append(msgs, val)
				}
				return msgs
			},
			wantVal: []*mq.Message{
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic",
				},
				{
					Value: []byte("1"),
					Key:   []byte("1"),
					Topic: "test_topic",
				},
				{
					Value: []byte("2"),
					Key:   []byte("2"),
					Topic: "test_topic",
				},
				{
					Value: []byte("3"),
					Key:   []byte("3"),
					Topic: "test_topic",
				},
				{
					Value: []byte("4"),
					Key:   []byte("4"),
					Topic: "test_topic",
				},
				{
					Value: []byte("5"),
					Key:   []byte("5"),
					Topic: "test_topic",
				},
			},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			gormMq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = gormMq.Topic(tc.topic, int(tc.partitions))
			require.NoError(t, err)
			p, err := gormMq.Producer(tc.topic)
			require.NoError(t, err)
			consumers := tc.consumers(gormMq)
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			var wg sync.WaitGroup
			for _, c := range consumers {
				newc := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					msgs := tc.consumerFunc(newc)
					ans = append(ans, msgs...)
				}()
			}
			for _, msg := range tc.input {
				_, err := p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			time.Sleep(10 * time.Second)
			err = gormMq.Close()
			require.NoError(t, err)
			wg.Wait()
			assert.ElementsMatch(t, tc.wantVal, ans)
			// 清理测试环境
			g.TearDownTest()
		})
	}
}

func (g *GormMQSuite) getTables(prefix string) ([]string, error) {
	sqlDB, err := g.db.DB()
	if err != nil {
		return nil, err
	}
	tableNames := make([]string, 0, 32)
	rows, err := sqlDB.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?", "test", fmt.Sprintf("%s%%", prefix))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

func (g *GormMQSuite) getValues(topic string, partition int) ([]*domain.Partition, error) {
	ans := make([]*domain.Partition, 0, 32)
	for i := 0; i < partition; i++ {
		tableName := fmt.Sprintf("%s_%d", topic, i)
		var ps []*domain.Partition
		g.db.Table(tableName).Find(&ps)

		ans = append(ans, ps...)
	}
	return ans, nil
}

func TestMq(t *testing.T) {
	suite.Run(t, &GormMQSuite{
		dsn: "root:root@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local",
	})
}
