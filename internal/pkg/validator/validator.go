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
