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

package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/ecodeclub/mq-api/internal/errs"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka/common"
	kafkago "github.com/segmentio/kafka-go"
)

// consumerChannel先默认1000
const msgChannelSize = 1000

type Consumer struct {
	topic   string
	groupID string

	reader *kafkago.Reader
	msgCh  chan *mq.Message

	closeCtx           context.Context
	closeCtxCancelFunc context.CancelFunc
	closeErr           error
	closeOnce          *sync.Once
}

func NewConsumer(address []string, topic, groupID string) *Consumer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Consumer{
		topic:   topic,
		groupID: groupID,
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: address,
			Topic:   topic,
			GroupID: groupID,
		}),
		msgCh:              make(chan *mq.Message, msgChannelSize),
		closeCtx:           ctx,
		closeCtxCancelFunc: cancelFunc,
		closeErr:           nil,
		closeOnce:          &sync.Once{},
	}
}

func (c *Consumer) Consume(ctx context.Context) (*mq.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m, ok := <-c.msgCh:
		if !ok {
			return nil, fmt.Errorf("kafka: %w", errs.ErrConsumerIsClosed)
		}
		return m, nil
	}
}

func (c *Consumer) ConsumeChan(ctx context.Context) (<-chan *mq.Message, error) {
	if c.closeCtx.Err() != nil {
		return nil, fmt.Errorf("kafka: %w", errs.ErrConsumerIsClosed)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return c.msgCh, nil
}

func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.closeCtxCancelFunc()
		c.closeErr = c.reader.Close()
	})
	return c.closeErr
}

// getMsgFromKafka 完成持续从kafka内获取数据
func (c *Consumer) getMsgFromKafka() {
	defer func() {
		close(c.msgCh)
	}()

	for {
		m, err := c.reader.ReadMessage(c.closeCtx)
		if err != nil {
			if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("读取消息失败: %s", err.Error())
			continue
		}
		msg := common.ConvertToMQMessage(m)
		select {
		case c.msgCh <- msg:
		case <-c.closeCtx.Done():
			return
		}
	}
}
