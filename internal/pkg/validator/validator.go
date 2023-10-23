package validator

import (
	"regexp"
	"strings"
)

const (
	maxTopicNameLength = 50
)

func IsValidTopic(name string) bool {
	// 检查名称长度
	if !(0 < len(name) && len(name) <= maxTopicNameLength) {
		return false
	}

	// 检查特殊字符是否作为开头
	if strings.HasPrefix(name, "_") || strings.HasPrefix(name, "-") || strings.HasPrefix(name, ".") {
		return false
	}

	// 使用正则表达式检查名称格式
	regex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_\-.]*[a-zA-Z0-9]$`)
	return regex.MatchString(name)
}
