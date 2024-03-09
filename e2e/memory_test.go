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
	"github.com/ecodeclub/mq-api/memory"
	"github.com/stretchr/testify/suite"
)

func TestMemory(t *testing.T) {
	suite.Run(t, NewTestSuite(
		&MemoryTestSuite{},
	))
}

type MemoryTestSuite struct{}

func (k *MemoryTestSuite) Create() mq.MQ {
	memoryMq := memory.NewMQ()

	return memoryMq
}

func (k *MemoryTestSuite) Ping(ctx context.Context) error {
	return nil
}
