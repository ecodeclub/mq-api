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
