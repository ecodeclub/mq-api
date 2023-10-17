#!/bin/sh

echo "运行集成测试中......"
make it
if [ $? -ne 0 ]; then
  echo "错误: 请在本地运行'make it'命令,确认测试全部通过后再提交." >&2
  exit 1
fi