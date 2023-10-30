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

package mqerr

import "errors"

var (
	ErrConsumerIsClosed = errors.New("消费者已经关闭")
	ErrProducerIsClosed = errors.New("生产者已经关闭")
	ErrMQIsClosed       = errors.New("mq已经关闭")
	ErrInvalidTopic     = errors.New("topic非法")
	ErrCreatedTopic     = errors.New("topic已创建")
	ErrInvalidPartition = errors.New("partition非法")
	ErrUnknownTopic     = errors.New("未知topic")
)
