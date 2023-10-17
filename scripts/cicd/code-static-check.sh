#!/bin/sh

output=$(make lint)
if [ -n "$output" ]; then
  echo "错误: 请在本地运行'make lint'命令,确认无误后再提交." >&2
  exit 1
fi