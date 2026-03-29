#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

DEFAULT_DB_PATH="${PROD_DATA_DIR}/a_stock_quant.db"
DEFAULT_OUTPUT_PATH="${HOME}/Downloads/a_stock_quant-$(date +%Y%m%d-%H%M%S).db"

usage() {
    cat <<EOF
用法:
  scripts/export_prod_db.sh [options]

说明:
  - 对当前生产 SQLite 数据库执行一致性快照导出
  - 默认导出到 ~/Downloads
  - 可选使用 zstd 压缩，适合大库传到服务器

选项:
  --db-path PATH         源数据库路径，默认 ${DEFAULT_DB_PATH}
  --output PATH          导出文件路径，默认 ${DEFAULT_OUTPUT_PATH}
  --compress MODE        压缩模式: auto | zstd | none，默认 auto
  --keep-uncompressed    压缩后保留未压缩的 .db 文件
  -h, --help             显示帮助

示例:
  ./scripts/export_prod_db.sh
  ./scripts/export_prod_db.sh --compress zstd
  ./scripts/export_prod_db.sh --db-path "./data/a_stock_quant.db" --output "/tmp/quant.db"
EOF
}

require_command() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "缺少命令: ${cmd}" >&2
        exit 1
    fi
}

escape_sqlite_path() {
    local path="$1"
    printf "%s" "${path//\'/\'\'}"
}

print_file_info() {
    local path="$1"
    local digest
    digest="$(hash_file_sha256 "${path}")"
    ls -lh "${path}"
    echo "sha256: ${digest}"
}

DB_PATH="${DEFAULT_DB_PATH}"
OUTPUT_PATH="${DEFAULT_OUTPUT_PATH}"
COMPRESS_MODE="auto"
KEEP_UNCOMPRESSED="false"

while [ $# -gt 0 ]; do
    case "$1" in
        --db-path)
            DB_PATH="${2:-}"
            shift 2
            ;;
        --output)
            OUTPUT_PATH="${2:-}"
            shift 2
            ;;
        --compress)
            COMPRESS_MODE="${2:-}"
            shift 2
            ;;
        --keep-uncompressed)
            KEEP_UNCOMPRESSED="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "未知参数: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

case "${COMPRESS_MODE}" in
    auto|zstd|none)
        ;;
    *)
        echo "不支持的压缩模式: ${COMPRESS_MODE}" >&2
        exit 1
        ;;
esac

require_command sqlite3

if [ ! -f "${DB_PATH}" ]; then
    echo "数据库文件不存在: ${DB_PATH}" >&2
    exit 1
fi

OUTPUT_DIR="$(dirname "${OUTPUT_PATH}")"
mkdir -p "${OUTPUT_DIR}"

if [ "${OUTPUT_PATH##*.}" = "zst" ]; then
    echo "--output 请传入 .db 路径，不要直接传 .zst" >&2
    exit 1
fi

ESCAPED_OUTPUT_PATH="$(escape_sqlite_path "${OUTPUT_PATH}")"

echo "开始导出数据库快照"
echo "源数据库: ${DB_PATH}"
echo "快照路径: ${OUTPUT_PATH}"

rm -f "${OUTPUT_PATH}"
sqlite3 "${DB_PATH}" ".backup '${ESCAPED_OUTPUT_PATH}'"

INTEGRITY_RESULT="$(sqlite3 "${OUTPUT_PATH}" "PRAGMA integrity_check;")"
if [ "${INTEGRITY_RESULT}" != "ok" ]; then
    echo "快照完整性检查失败: ${INTEGRITY_RESULT}" >&2
    exit 1
fi

FINAL_OUTPUT_PATH="${OUTPUT_PATH}"

case "${COMPRESS_MODE}" in
    auto)
        if command -v zstd >/dev/null 2>&1; then
            COMPRESS_MODE="zstd"
        else
            COMPRESS_MODE="none"
        fi
        ;;
esac

if [ "${COMPRESS_MODE}" = "zstd" ]; then
    require_command zstd
    FINAL_OUTPUT_PATH="${OUTPUT_PATH}.zst"
    echo "使用 zstd 压缩快照 -> ${FINAL_OUTPUT_PATH}"
    zstd -T0 -3 -f "${OUTPUT_PATH}" -o "${FINAL_OUTPUT_PATH}"
    if [ "${KEEP_UNCOMPRESSED}" != "true" ]; then
        rm -f "${OUTPUT_PATH}"
    fi
fi

echo
echo "导出完成"
print_file_info "${FINAL_OUTPUT_PATH}"

cat <<EOF

推荐上传命令:
  rsync -avP --partial "${FINAL_OUTPUT_PATH}" deploy@<你的服务器IP>:/home/deploy/
EOF
