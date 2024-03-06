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
