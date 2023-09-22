package domain

import (
	"gorm.io/gorm"
)

type Partition struct {
	gorm.Model
	Value  string `gorm:"column:value;not null"`
	Key    string `gorm:"column:key;not null"`
	Header string `gorm:"column:header;not null"`
	Topic  string `gorm:"column:topic;not null"`
}

type Cursors struct {
	gorm.Model
	Table         string `gorm:"column:table;type:varchar(255);not null"`
	Cursor        int64  `gorm:"column:cursor;type:int(11);not null"`
	ConsumerGroup string `gorm:"column:consumer_group;type:varchar(255);not null"`
	Occupant      string `gorm:"column:occupant;type:varchar(255);not null"`
}

func (c *Cursors) TableName() string {
	return "cursors"
}
