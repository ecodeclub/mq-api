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
