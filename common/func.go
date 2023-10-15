package common

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ecodeclub/mq-api"
)

func ConvertHeaderSliceToMap(headers []kafka.Header) mq.Header {
	headerMap := mq.Header{}
	for _, header := range headers {
		key := header.Key
		value := string(header.Value)
		headerMap[key] = value
	}

	return headerMap
}

func ConvertHeaderMap(headerMap mq.Header) []kafka.Header {
	headers := make([]kafka.Header, 0, len(headerMap))
	for key, value := range headerMap {
		header := kafka.Header{
			Key:   key,
			Value: []byte(value),
		}
		headers = append(headers, header)
	}
	return headers
}
