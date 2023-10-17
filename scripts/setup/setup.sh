#!/bin/bash
# Copyright 2021 ecodeclub
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Check Go installation
echo "检查 Go版本、Docker、Docker Compose V2......"
GO_VERSION="1.21"  # Specify the required Go version
if ! command -v go >/dev/null || [[ ! "$(go version | awk '{print $3}')" == *"$GO_VERSION"* ]]; then
    echo "Go $GO_VERSION 未安装或者版本不正确"
    exit 1  # 退出并返回错误代码
fi

if ! command -v docker >/dev/null; then
    echo "Docker 未安装"
    exit 1  # Exit with an error code
fi

if ! command -v docker compose >/dev/null; then
    echo "Docker Compose V2未安装"
    exit 1  # Exit with an error code
fi

DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_COMMIT=$DIR/git/pre-commit
TARGET_COMMIT=.git/hooks/pre-commit
SOURCE_PUSH=$DIR/git/pre-push
TARGET_PUSH=.git/hooks/pre-push

# copy pre-commit file if not exist.
echo "设置 git pre-commit hooks..."
cp "$SOURCE_COMMIT" $TARGET_COMMIT

# copy pre-push file if not exist.
echo "设置 git pre-push hooks..."
cp "$SOURCE_PUSH" $TARGET_PUSH

# add permission to TARGET_PUSH and TARGET_COMMIT file.
test -x $TARGET_PUSH || chmod +x $TARGET_PUSH
test -x $TARGET_COMMIT || chmod +x $TARGET_COMMIT

echo "安装 golangci-lint......"
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.54.2

echo "安装 goimports......"
go install golang.org/x/tools/cmd/goimports@latest

echo "安装 gofumpt......"
go install mvdan.cc/gofumpt@latest