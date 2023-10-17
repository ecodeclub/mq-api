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

package mq

import "context"

type Header map[string]string

type Message struct {
	Value  []byte
	Key    []byte
	Header Header
	Topic  string
}

type ProducerResult struct{}

type Producer interface {
	Produce(ctx context.Context, m *Message) (*ProducerResult, error)
}

type Consumer interface {
	Consume(ctx context.Context) (*Message, error)
	ConsumeMsgCh(ctx context.Context) (<-chan *Message, error)
}

type MQ interface {
	Producer(topic string) Producer
	Consumer(topic string) Consumer
}
