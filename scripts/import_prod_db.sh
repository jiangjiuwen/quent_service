#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

TARGET_DB_PATH="${PROD_DATA_DIR}/a_stock_quant.db"

usage() {
    cat <<EOF
用法:
  scripts/import_prod_db.sh --source PATH [options]

说明:
  - 将 SQLite 快照导入到生产数据目录
  - 如果生产服务已部署，会先停服务，导入后再启动并检查健康状态
  - 支持导入 .db 或 .db.zst

选项:
  --source PATH      要导入的数据库快照路径，必填
  --skip-backup      导入前不备份现有数据库
  --keep-source      导入完成后保留源文件
  --no-restart       导入后不自动启动服务
  -h, --help         显示帮助

示例:
  ./scripts/import_prod_db.sh --source /home/deploy/a_stock_quant-20260329-010203.db
  ./scripts/import_prod_db.sh --source /home/deploy/a_stock_quant-20260329-010203.db.zst
EOF
}

require_command() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "缺少命令: ${cmd}" >&2
        exit 1
    fi
}

service_is_deployed() {
    [ -L "${PROD_APP_DIR}" ] || [ -f "${PROD_ENV_FILE}" ]
}

remove_sqlite_sidecars() {
    rm -f "${TARGET_DB_PATH}-wal" "${TARGET_DB_PATH}-shm"
}

start_service_if_needed() {
    if [ "${AUTO_RESTART}" != "true" ]; then
        return 0
    fi

    if ! service_is_deployed; then
        return 0
    fi

    "${WORKSPACE_DIR}/scripts/prod_ctl.sh" start
    "${WORKSPACE_DIR}/scripts/prod_ctl.sh" status
}

stop_service_if_needed() {
    if ! service_is_deployed; then
        return 0
    fi

    "${WORKSPACE_DIR}/scripts/prod_ctl.sh" stop || true
}

verify_integrity() {
    local db_path="$1"
    local result
    result="$(sqlite3 "${db_path}" "PRAGMA integrity_check;")"
    if [ "${result}" != "ok" ]; then
        echo "数据库完整性检查失败: ${result}" >&2
        exit 1
    fi
}

SOURCE_PATH=""
SKIP_BACKUP="false"
KEEP_SOURCE="false"
AUTO_RESTART="true"

while [ $# -gt 0 ]; do
    case "$1" in
        --source)
            SOURCE_PATH="${2:-}"
            shift 2
            ;;
        --skip-backup)
            SKIP_BACKUP="true"
            shift
            ;;
        --keep-source)
            KEEP_SOURCE="true"
            shift
            ;;
        --no-restart)
            AUTO_RESTART="false"
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

if [ -z "${SOURCE_PATH}" ]; then
    echo "请通过 --source 指定导入文件" >&2
    usage >&2
    exit 1
fi

if [ ! -f "${SOURCE_PATH}" ]; then
    echo "导入文件不存在: ${SOURCE_PATH}" >&2
    exit 1
fi

require_command sqlite3
mkdir -p "${PROD_DATA_DIR}" "${PROD_TMP_DIR}"

STAMP="$(date +%Y%m%d-%H%M%S)"
TMP_DB_PATH="${PROD_TMP_DIR}/a_stock_quant.import-${STAMP}.db"
BACKUP_DB_PATH="${PROD_DATA_DIR}/a_stock_quant.pre-import-${STAMP}.db"

echo "准备导入数据库"
echo "源文件: ${SOURCE_PATH}"
echo "目标库: ${TARGET_DB_PATH}"

stop_service_if_needed
remove_sqlite_sidecars

case "${SOURCE_PATH}" in
    *.zst)
        require_command zstd
        echo "检测到 zstd 压缩快照，开始解压"
        rm -f "${TMP_DB_PATH}"
        zstd -d -f "${SOURCE_PATH}" -o "${TMP_DB_PATH}"
        if [ "${KEEP_SOURCE}" != "true" ]; then
            rm -f "${SOURCE_PATH}"
        fi
        ;;
    *.db)
        if [ "${KEEP_SOURCE}" = "true" ]; then
            cp -f "${SOURCE_PATH}" "${TMP_DB_PATH}"
        else
            mv -f "${SOURCE_PATH}" "${TMP_DB_PATH}"
        fi
        ;;
    *)
        echo "只支持导入 .db 或 .db.zst 文件" >&2
        exit 1
        ;;
esac

verify_integrity "${TMP_DB_PATH}"

if [ -f "${TARGET_DB_PATH}" ] && [ "${SKIP_BACKUP}" != "true" ]; then
    echo "备份现有数据库 -> ${BACKUP_DB_PATH}"
    mv -f "${TARGET_DB_PATH}" "${BACKUP_DB_PATH}"
fi

rm -f "${TARGET_DB_PATH}"
mv -f "${TMP_DB_PATH}" "${TARGET_DB_PATH}"
remove_sqlite_sidecars

if [ -n "${PROD_SERVICE_USER}" ]; then
    chown "${PROD_SERVICE_USER}:${PROD_SERVICE_GROUP}" "${TARGET_DB_PATH}" 2>/dev/null || true
fi

echo
echo "导入完成"
ls -lh "${TARGET_DB_PATH}"
echo "sha256: $(hash_file_sha256 "${TARGET_DB_PATH}")"

start_service_if_needed

cat <<EOF

后续验证命令:
  cd "${WORKSPACE_DIR}"
  ./scripts/prod_ctl.sh status
  curl "http://${PROD_API_HOST}:${PROD_API_PORT}/health"
EOF
