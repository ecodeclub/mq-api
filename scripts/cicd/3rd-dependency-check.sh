#!/bin/sh

output=$(make tidy)
if [ -n "$output" ]; then
  echo "错误: 请在本地运行'make tidy'命令,确认无误后再提交." >&2
  exit 1
fi