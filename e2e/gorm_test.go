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

func (g *GormMQSuite) TestProducer() {
	testcases := []struct {
		name    string
		topic   string
		input   []*mq.Message
		wantVal []*domain.Partition
	}{
		{
			name:  "发送3条消息",
			topic: "test_topic",
			input: []*mq.Message{
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
			},
			wantVal: []*domain.Partition{
				{
					Value:  "1",
					Key:    "1",
					Topic:  "test_topic",
					Header: "null",
				},
				{
					Value:  "2",
					Key:    "2",
					Topic:  "test_topic",
					Header: "null",
				},
				{
					Value:  "3",
					Key:    "3",
					Topic:  "test_topic",
					Header: "null",
				},
			},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			mq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = mq.Topic(tc.topic, 4)
			require.NoError(t, err)
			p, err := mq.Producer(tc.topic)
			require.NoError(t, err)
			for _, msg := range tc.input {
				_, err = p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			ans, err := g.getValues(tc.topic, 4)
			assert.Equal(t, tc.wantVal, ans)
		})
	}
}

func (g *GormMQSuite) TestConsumer() {
	testcases := []struct {
		name    string
		topic   string
		input   []*mq.Message
		wantVal []*mq.Message
	}{
		{
			name:  "消费消息",
			topic: "test_topic",
			input: []*mq.Message{
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
			},
		},
	}
	for _, tc := range testcases {
		g.T().Run(tc.name, func(t *testing.T) {
			gormMq, err := gorm_mq.NewMq(g.db)
			require.NoError(t, err)
			err = gormMq.Topic(tc.topic, 4)
			require.NoError(t, err)
			p, err := gormMq.Producer(tc.topic)
			require.NoError(t, err)
			c1, err := gormMq.Consumer(tc.topic)
			for _, msg := range tc.input {
				_, err = p.Produce(context.Background(), msg)
				require.NoError(t, err)
			}
			msgCh, err := c1.ConsumeMsgCh(context.Background())
			require.NoError(t, err)
			ans := make([]*mq.Message, 0, len(tc.wantVal))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for {
				select {
				case msg := <-msgCh:
					ans = append(ans, msg)
					if len(ans) == len(tc.wantVal) {
						break
					}
				case <-ctx.Done():
				}
				if ctx.Err() != nil {
					break
				}
			}
			assert.Equal(t, tc.wantVal, ans)
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
