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

package common

import (
	"github.com/ecodeclub/mq-api"
	"github.com/segmentio/kafka-go"
)

func ConvertToMqMsg(kafkaMsg kafka.Message) *mq.Message {
	header := mq.Header{}
	for _, h := range kafkaMsg.Headers {
		header[h.Key] = string(h.Value)
	}
	return &mq.Message{
		Key:         kafkaMsg.Key,
		Value:       kafkaMsg.Value,
		Topic:       kafkaMsg.Topic,
		Header:      header,
		PartitionID: int64(kafkaMsg.Partition),
		Offset:      kafkaMsg.Offset,
	}
}

func ConvertToKafkaHeader(header mq.Header) []kafka.Header {
	h := make([]kafka.Header, 0, len(header))
	for key, val := range header {
		h = append(h, kafka.Header{
			Key:   key,
			Value: []byte(val),
		})
	}
	return h
}
