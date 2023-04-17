#!/bin/zsh

# 获取脚本所在文件夹路径
SCRIPT_DIR=$(dirname "$0")

# 统计代码行数
find "$SCRIPT_DIR" \( -name "*.go" \) -exec wc -l {} +

# 格式化代码
gofmt -w "$SCRIPT_DIR" 

echo "已完成上面的文件的代码格式化"
