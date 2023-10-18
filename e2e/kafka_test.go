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

//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/ecodeclub/mq-api"
	"github.com/ecodeclub/mq-api/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/suite"
)

func TestKafka(t *testing.T) {
	address := []string{"127.0.0.1:9092"}
	testMq, err := kafka.NewMQ("tcp", address)
	if err != nil {
		panic(err)
	}
	suite.Run(t, NewBaseSuite(
		KafkaTestSuite{address: address},
		testMq,
	))
}

type KafkaTestSuite struct {
	address []string
}

func (k KafkaTestSuite) Init() mq.MQ {
	kafkaMq, err := kafka.NewMQ("tcp", k.address)
	if err != nil {
		panic(err)
	}
	return kafkaMq
}

func (k KafkaTestSuite) Ping(ctx context.Context) error {
	w := kafkago.Writer{
		Addr:                   kafkago.TCP(k.address...),
		Topic:                  "topic-A",
		AllowAutoTopicCreation: true,
	}
	err := w.WriteMessages(ctx, kafkago.Message{
		Key:   []byte("1"),
		Value: []byte("1"),
	})
	return err
}
