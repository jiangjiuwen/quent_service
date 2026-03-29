#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

TARGET_DB_PATH="${PROD_DATA_DIR}/a_stock_quant.db"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

elapsed_seconds() {
    local start_ts="$1"
    local end_ts
    end_ts="$(date +%s)"
    printf '%ss' "$((end_ts - start_ts))"
}

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
        log "跳过服务启动: 已指定 --no-restart"
        return 0
    fi

    if ! service_is_deployed; then
        log "跳过服务启动: 当前还没有已部署的生产服务"
        return 0
    fi

    log "开始启动生产服务"
    "${WORKSPACE_DIR}/scripts/prod_ctl.sh" start
    "${WORKSPACE_DIR}/scripts/prod_ctl.sh" status
    log "生产服务启动检查完成"
}

stop_service_if_needed() {
    if ! service_is_deployed; then
        log "跳过停服务: 当前还没有已部署的生产服务"
        return 0
    fi

    log "开始停止生产服务"
    "${WORKSPACE_DIR}/scripts/prod_ctl.sh" stop || true
    log "生产服务停止完成"
}

verify_integrity() {
    local db_path="$1"
    local start_ts
    local result
    start_ts="$(date +%s)"
    log "开始执行 SQLite 完整性检查，这一步通常最耗时"
    result="$(sqlite3 "${db_path}" "PRAGMA integrity_check;")"
    if [ "${result}" != "ok" ]; then
        echo "数据库完整性检查失败: ${result}" >&2
        exit 1
    fi
    log "SQLite 完整性检查通过，耗时 $(elapsed_seconds "${start_ts}")"
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
IMPORT_START_TS="$(date +%s)"

log "准备导入数据库"
log "源文件: ${SOURCE_PATH}"
ls -lh "${SOURCE_PATH}"
log "目标库: ${TARGET_DB_PATH}"

stop_service_if_needed
remove_sqlite_sidecars

case "${SOURCE_PATH}" in
    *.zst)
        DECOMPRESS_START_TS="$(date +%s)"
        require_command zstd
        log "检测到 zstd 压缩快照，开始解压到临时文件"
        rm -f "${TMP_DB_PATH}"
        zstd -d -f "${SOURCE_PATH}" -o "${TMP_DB_PATH}"
        log "解压完成，耗时 $(elapsed_seconds "${DECOMPRESS_START_TS}")"
        ls -lh "${TMP_DB_PATH}"
        if [ "${KEEP_SOURCE}" != "true" ]; then
            rm -f "${SOURCE_PATH}"
        fi
        ;;
    *.db)
        PREPARE_START_TS="$(date +%s)"
        log "检测到未压缩数据库快照，开始准备临时文件"
        if [ "${KEEP_SOURCE}" = "true" ]; then
            cp -f "${SOURCE_PATH}" "${TMP_DB_PATH}"
        else
            mv -f "${SOURCE_PATH}" "${TMP_DB_PATH}"
        fi
        log "临时文件准备完成，耗时 $(elapsed_seconds "${PREPARE_START_TS}")"
        ls -lh "${TMP_DB_PATH}"
        ;;
    *)
        echo "只支持导入 .db 或 .db.zst 文件" >&2
        exit 1
        ;;
esac

verify_integrity "${TMP_DB_PATH}"

if [ -f "${TARGET_DB_PATH}" ] && [ "${SKIP_BACKUP}" != "true" ]; then
    log "开始备份现有数据库 -> ${BACKUP_DB_PATH}"
    mv -f "${TARGET_DB_PATH}" "${BACKUP_DB_PATH}"
    log "现有数据库备份完成"
elif [ "${SKIP_BACKUP}" = "true" ]; then
    log "跳过备份: 已指定 --skip-backup"
else
    log "跳过备份: 当前目标数据库不存在"
fi

log "开始切换新的生产数据库"
rm -f "${TARGET_DB_PATH}"
mv -f "${TMP_DB_PATH}" "${TARGET_DB_PATH}"
remove_sqlite_sidecars
log "生产数据库切换完成"

if [ -n "${PROD_SERVICE_USER}" ]; then
    chown "${PROD_SERVICE_USER}:${PROD_SERVICE_GROUP}" "${TARGET_DB_PATH}" 2>/dev/null || true
    log "已修正数据库文件属主为 ${PROD_SERVICE_USER}:${PROD_SERVICE_GROUP}"
fi

echo
log "导入完成，总耗时 $(elapsed_seconds "${IMPORT_START_TS}")"
ls -lh "${TARGET_DB_PATH}"
echo "sha256: $(hash_file_sha256 "${TARGET_DB_PATH}")"

start_service_if_needed

cat <<EOF

后续验证命令:
  cd "${WORKSPACE_DIR}"
  ./scripts/prod_ctl.sh status
  curl "http://${PROD_API_HOST}:${PROD_API_PORT}/health"
EOF
