#!/bin/sh

output=$(make fmt)
if [ -n "$output" ]; then
  echo >&2 "错误: 请在本地运行'make fmt'命令,确认无误后再提交."
  exit 1
fi