#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

REQUIREMENTS_HASH_FILE="${PROD_ROOT}/requirements.sha256"

usage() {
    cat <<EOF
用法:
  scripts/prod_deploy.sh

说明:
  - 将当前工作区复制为独立生产快照
  - 在 ${PROD_ROOT} 下创建/复用生产虚拟环境、数据库和日志
  - 安装或更新 launchd 常驻服务
  - 重启生产 API 服务
EOF
}

job_loaded() {
    local label="$1"
    launchctl print "${GUI_DOMAIN}/${label}" >/dev/null 2>&1
}

job_pid() {
    local label="$1"
    launchctl print "${GUI_DOMAIN}/${label}" 2>/dev/null | awk -F'= ' '/pid =/ {gsub(/^[[:space:]]+/, "", $2); print $2; exit}'
}

ensure_loaded() {
    local label="$1"
    local plist="$2"

    if job_loaded "${label}"; then
        return 0
    fi

    launchctl bootstrap "${GUI_DOMAIN}" "${plist}"
}

acquire_lock() {
    mkdir -p "${PROD_ROOT}"
    if ! mkdir "${PROD_DEPLOY_LOCK_DIR}" 2>/dev/null; then
        echo "检测到另一个部署正在执行: ${PROD_DEPLOY_LOCK_DIR}" >&2
        exit 1
    fi
    trap 'rmdir "${PROD_DEPLOY_LOCK_DIR}"' EXIT
}

prepare_directories() {
    mkdir -p \
        "${PROD_RELEASES_DIR}" \
        "${PROD_DATA_DIR}" \
        "${PROD_LOG_DIR}" \
        "${PROD_TMP_DIR}" \
        "${LAUNCH_AGENTS_DIR}"

    touch \
        "${SERVICE_STDOUT_LOG}" \
        "${SERVICE_STDERR_LOG}" \
        "${WATCH_STDOUT_LOG}" \
        "${WATCH_STDERR_LOG}"
}

create_release() {
    local release_id release_dir
    release_id="$(date +%Y%m%dT%H%M%S)-$$"
    release_dir="${PROD_RELEASES_DIR}/${release_id}"
    mkdir -p "${release_dir}"

    rsync -a --delete \
        --exclude '.venv/' \
        --exclude 'data/' \
        --exclude 'logs/' \
        --exclude '__pycache__/' \
        --exclude '*.pyc' \
        --exclude '*.pyo' \
        --exclude '.DS_Store' \
        --exclude '.pytest_cache/' \
        --exclude '.mypy_cache/' \
        --exclude '.ruff_cache/' \
        --exclude '.git/' \
        "${WORKSPACE_DIR}/" "${release_dir}/"

    echo "${release_dir}"
}

ensure_venv() {
    local desired_version current_version

    desired_version="$("${PROD_PYTHON}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    echo "使用生产 Python 解释器: ${PROD_PYTHON} (version ${desired_version})"

    if [ -x "${PROD_VENV_DIR}/bin/python" ]; then
        current_version="$("${PROD_VENV_DIR}/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || true)"
        if [ "${current_version}" != "${desired_version}" ]; then
            echo "检测到生产虚拟环境版本不兼容: ${current_version:-unknown} -> ${desired_version}，重建 venv"
            rm -rf "${PROD_VENV_DIR}"
        fi
    fi

    if [ ! -x "${PROD_VENV_DIR}/bin/python" ]; then
        "${PROD_PYTHON}" -m venv "${PROD_VENV_DIR}"
    fi
}

sync_dependencies() {
    local release_dir="$1"
    local new_hash existing_hash

    new_hash="$(shasum -a 256 "${release_dir}/requirements.txt" | awk '{print $1}')"
    if [ -f "${REQUIREMENTS_HASH_FILE}" ]; then
        existing_hash="$(cat "${REQUIREMENTS_HASH_FILE}")"
    else
        existing_hash=""
    fi

    if [ "${new_hash}" = "${existing_hash}" ]; then
        echo "requirements.txt 未变化，跳过依赖安装"
        return 0
    fi

    "${PROD_VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel
    "${PROD_VENV_DIR}/bin/pip" install -r "${release_dir}/requirements.txt"
    printf '%s\n' "${new_hash}" > "${REQUIREMENTS_HASH_FILE}"
}

switch_release() {
    local release_dir="$1"
    local legacy_dir

    if [ -e "${PROD_APP_DIR}" ] && [ ! -L "${PROD_APP_DIR}" ]; then
        legacy_dir="${PROD_RELEASES_DIR}/legacy-current-$(date +%Y%m%dT%H%M%S)"
        mv "${PROD_APP_DIR}" "${legacy_dir}"
    fi

    ln -sfn "${release_dir}" "${PROD_APP_DIR}"
}

write_env_file() {
    cat > "${PROD_ENV_FILE}" <<EOF
export PYTHONUNBUFFERED="1"
export QUANT_BASE_DIR="${PROD_APP_DIR}"
export QUANT_DATA_DIR="${PROD_DATA_DIR}"
export QUANT_LOG_DIR="${PROD_LOG_DIR}"
export QUANT_WEB_DIR="${PROD_APP_DIR}/web"
export QUANT_WEB_ASSETS_DIR="${PROD_APP_DIR}/web/assets"
export QUANT_DB_PATH="${PROD_DATA_DIR}/a_stock_quant.db"
export QUANT_API_HOST="${PROD_API_HOST}"
export QUANT_API_PORT="${PROD_API_PORT}"
export QUANT_BATCH_SIZE="${PROD_BATCH_SIZE}"
export QUANT_SYNC_HOUR="${PROD_SYNC_HOUR}"
export QUANT_SYNC_MINUTE="${PROD_SYNC_MINUTE}"
export QUANT_HISTORY_YEARS="${PROD_HISTORY_YEARS}"
export QUANT_START_YEAR="${PROD_START_YEAR}"
export QUANT_LOG_LEVEL="${PROD_LOG_LEVEL}"
export QUANT_WATCH_POLL_SECONDS="${WATCH_POLL_SECONDS}"
export QUANT_WATCH_DEBOUNCE_SECONDS="${WATCH_DEBOUNCE_SECONDS}"
export QUANT_WATCH_STATE_FILE="${WATCH_STATE_FILE}"
EOF
}

write_service_plist() {
    cat > "${PROD_SERVICE_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PROD_SERVICE_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-lc</string>
        <string>source "${PROD_ENV_FILE}"; cd "${PROD_APP_DIR}"; exec "${PROD_VENV_DIR}/bin/python" "${PROD_APP_DIR}/main.py"</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${PROD_APP_DIR}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>${SERVICE_STDOUT_LOG}</string>
    <key>StandardErrorPath</key>
    <string>${SERVICE_STDERR_LOG}</string>
</dict>
</plist>
EOF
}

write_watch_plist() {
    cat > "${PROD_WATCH_PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PROD_WATCH_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-lc</string>
        <string>source "${PROD_ENV_FILE}"; export QUANT_WATCH_WORKSPACE_DIR="${WORKSPACE_DIR}"; export QUANT_WATCH_DEPLOY_SCRIPT="${WORKSPACE_DIR}/scripts/prod_deploy.sh"; exec "${PROD_VENV_DIR}/bin/python" "${WORKSPACE_DIR}/scripts/prod_watch.py"</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${WORKSPACE_DIR}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>${WATCH_STDOUT_LOG}</string>
    <key>StandardErrorPath</key>
    <string>${WATCH_STDERR_LOG}</string>
</dict>
</plist>
EOF
}

initialize_database() {
    (
        source "${PROD_ENV_FILE}"
        cd "${PROD_APP_DIR}"
        "${PROD_VENV_DIR}/bin/python" -c 'from database.connection import db; db.init_tables()'
    )
}

restart_service() {
    ensure_loaded "${PROD_SERVICE_LABEL}" "${PROD_SERVICE_PLIST}"
    launchctl kickstart -k "${GUI_DOMAIN}/${PROD_SERVICE_LABEL}"
}

cleanup_obsolete_backfill_job() {
    if job_loaded "${PROD_BACKFILL_LABEL}"; then
        launchctl bootout "${GUI_DOMAIN}/${PROD_BACKFILL_LABEL}" >/dev/null 2>&1 || true
    fi

    rm -f \
        "${PROD_BACKFILL_PLIST}" \
        "${BACKFILL_STATE_FILE}" \
        "${BACKFILL_STDOUT_LOG}" \
        "${BACKFILL_STDERR_LOG}"
}

cleanup_old_releases() {
    local current_release=""
    current_release="$(readlink "${PROD_APP_DIR}" 2>/dev/null || true)"

    ls -1d "${PROD_RELEASES_DIR}"/* 2>/dev/null | sort -r | awk "NR>${KEEP_RELEASES}" | while IFS= read -r old_release; do
        if [ -n "${current_release}" ] && [ "${old_release}" = "${current_release}" ]; then
            continue
        fi
        rm -rf "${old_release}"
    done || true
}

wait_for_health() {
    local attempt
    for attempt in $(seq 1 30); do
        if curl --silent --show-error --fail "http://${PROD_API_HOST}:${PROD_API_PORT}/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "生产服务健康检查失败: http://${PROD_API_HOST}:${PROD_API_PORT}/health" >&2
    return 1
}

while [ $# -gt 0 ]; do
    case "$1" in
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
    shift
done

acquire_lock
prepare_directories
RELEASE_DIR="$(create_release)"
ensure_venv
sync_dependencies "${RELEASE_DIR}"
switch_release "${RELEASE_DIR}"
write_env_file
write_service_plist
write_watch_plist
initialize_database
cleanup_obsolete_backfill_job
restart_service
wait_for_health
cleanup_old_releases

echo "生产部署完成"
echo "API: http://${PROD_API_HOST}:${PROD_API_PORT}"
echo "服务标签: ${PROD_SERVICE_LABEL}"
echo "自动同步: 工作日 ${PROD_SYNC_HOUR}:$(printf '%02d' "${PROD_SYNC_MINUTE}")"
