package mqerr

import "errors"

var (
	ErrOccupierNotFound = errors.New("占有者找不到")
	ErrOccupied         = errors.New("已经被占用")
)
