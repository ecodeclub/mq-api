package e2e

import (
	"github.com/ecodeclub/mq-api/kafka"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestMq(t *testing.T) {
	q, err := kafka.NewMQ([]string{"127.0.0.1:9092"})
	require.NoError(t, err)
	suite.Run(t, NewBaseSuite(q))
}
