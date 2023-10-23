package validator_test

import (
	"testing"

	"github.com/ecodeclub/mq-api/internal/pkg/validator"
	"github.com/stretchr/testify/assert"
)

func TestIsValidTopic(t *testing.T) {
	t.Parallel()

	t.Run("合法Topic", func(t *testing.T) {
		t.Parallel()

		validTopics := []string{
			"topicName",
			"my-topic",
			"other_.-topic",
			"prefix.sub-topic",
		}

		for _, topic := range validTopics {
			topic := topic
			t.Run(topic, func(t *testing.T) {
				t.Parallel()
				assert.True(t, validator.IsValidTopic(topic), "Expected topic to be valid: %s", topic)
			})
		}
	})

	t.Run("非法Topic", func(t *testing.T) {
		t.Parallel()

		invalidTopics := []string{
			"",               // 空字符串
			".invalid.topic", // . 作为开头
			"-invalid-topic", // - 作为开头
			"_invalid_topic", // _ 作为开头
			"123-topic",      // 数字开头
			"topicName-",     // - 作为结尾
			"topicName.",     // . 作为结尾
			"topicName_",     // _ 作为结尾
			"topic Name",     // 包含空格
			"topic!",         // 包含非法字符
			"topic-name-is-too-long-topic-name-is-too-long-topic-name-is-too-long", // 超过最大长度
		}

		for _, topic := range invalidTopics {
			topic := topic
			t.Run(topic, func(t *testing.T) {
				t.Parallel()
				assert.False(t, validator.IsValidTopic(topic), "Expected topic to be invalid: %s", topic)
			})
		}
	})
}
