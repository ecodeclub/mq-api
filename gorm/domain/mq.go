package domain

import (
	"gorm.io/gorm"
	"time"
)

type MqMessage struct {
	gorm.Model
	Value     string
	Key       string
	Header    string
	Topic     string
	CreatedAt time.Time
}
