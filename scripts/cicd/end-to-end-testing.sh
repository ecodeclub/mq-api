#!/bin/sh

echo "运行端到端测试中......"
make e2e
if [ $? -ne 0 ]; then
  echo "错误: 请在本地运行'make e2e'命令,确认测试全部通过后再提交." >&2
  exit 1
fi
