#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

if [ "${OS_NAME}" != "Linux" ]; then
    echo "prod_deploy_linux.sh 仅支持 Linux" >&2
    exit 1
fi

REQUIREMENTS_HASH_FILE="${PROD_ROOT}/requirements.sha256"

usage() {
    cat <<EOF
用法:
  scripts/prod_deploy.sh

说明:
  - 将当前工作区复制为独立生产快照
  - 在 ${PROD_ROOT} 下创建/复用生产虚拟环境、数据库和日志
  - 自动写入 systemd 服务单元并设置开机自启
  - 重启生产 API 服务

环境变量:
  QUANT_PROD_ROOT           生产根目录，默认 ${PROD_ROOT}
  QUANT_PROD_SERVICE_NAME   systemd 服务名，默认 ${PROD_SERVICE_UNIT_NAME}
  QUANT_PROD_SERVICE_USER   运行服务的 Linux 用户，默认 ${PROD_SERVICE_USER}
EOF
}

ensure_systemd() {
    if ! command -v systemctl >/dev/null 2>&1; then
        echo "未找到 systemctl，当前 Linux 环境无法使用 systemd 部署" >&2
        exit 1
    fi
}

run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
        return 0
    fi

    if command -v sudo >/dev/null 2>&1; then
        sudo "$@"
        return 0
    fi

    echo "写入 ${SYSTEMD_UNIT_DIR} 需要 root 权限，请使用 root 执行或安装 sudo" >&2
    exit 1
}

systemctl_cmd() {
    ensure_systemd
    run_as_root systemctl "$@"
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
        "${PROD_TMP_DIR}"
}

create_release() {
    local release_id release_dir
    release_id="$(date +%Y%m%dT%H%M%S)-$$"
    release_dir="${PROD_RELEASES_DIR}/${release_id}"
    mkdir -p "${release_dir}"

    copy_workspace_to_release "${release_dir}"

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

    new_hash="$(hash_file_sha256 "${release_dir}/requirements.txt")"
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
PYTHONUNBUFFERED=1
QUANT_BASE_DIR="${PROD_APP_DIR}"
QUANT_DATA_DIR="${PROD_DATA_DIR}"
QUANT_LOG_DIR="${PROD_LOG_DIR}"
QUANT_WEB_DIR="${PROD_APP_DIR}/web"
QUANT_WEB_ASSETS_DIR="${PROD_APP_DIR}/web/assets"
QUANT_DB_PATH="${PROD_DATA_DIR}/a_stock_quant.db"
QUANT_API_HOST="${PROD_API_HOST}"
QUANT_API_PORT=${PROD_API_PORT}
QUANT_BATCH_SIZE=${PROD_BATCH_SIZE}
QUANT_SYNC_HOUR=${PROD_SYNC_HOUR}
QUANT_SYNC_MINUTE=${PROD_SYNC_MINUTE}
QUANT_HISTORY_YEARS=${PROD_HISTORY_YEARS}
QUANT_START_YEAR=${PROD_START_YEAR}
QUANT_END_YEAR=${PROD_END_YEAR}
QUANT_LOG_LEVEL="${PROD_LOG_LEVEL}"
QUANT_LOG_FORMAT="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}"
QUANT_SOURCE_AKSHARE_ENABLED=true
QUANT_SOURCE_BAOSTOCK_ENABLED=true
QUANT_SOURCE_TUSHARE_ENABLED=false
EOF
}

write_service_unit() {
    local unit_tmp="${PROD_TMP_DIR}/${PROD_SERVICE_UNIT_NAME}.tmp"

    cat > "${unit_tmp}" <<EOF
[Unit]
Description=A股量化数据服务
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${PROD_SERVICE_USER}
Group=${PROD_SERVICE_GROUP}
WorkingDirectory=${PROD_APP_DIR}
EnvironmentFile=${PROD_ENV_FILE}
ExecStartPre=${PROD_VENV_DIR}/bin/python ${PROD_APP_DIR}/scripts/prod_migrate.py
ExecStart=${PROD_VENV_DIR}/bin/python ${PROD_APP_DIR}/main.py
Restart=always
RestartSec=10
TimeoutStopSec=20
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    run_as_root install -d -m 0755 "${SYSTEMD_UNIT_DIR}"
    run_as_root install -m 0644 "${unit_tmp}" "${PROD_SERVICE_UNIT}"
    rm -f "${unit_tmp}"
}

initialize_database() {
    (
        load_prod_env
        cd "${PROD_APP_DIR}"
        "${PROD_VENV_DIR}/bin/python" "${PROD_APP_DIR}/scripts/prod_migrate.py"
    )
}

restart_service() {
    systemctl_cmd daemon-reload
    systemctl_cmd enable "${PROD_SERVICE_UNIT_NAME}"
    systemctl_cmd restart "${PROD_SERVICE_UNIT_NAME}"
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

ensure_systemd
acquire_lock
prepare_directories
RELEASE_DIR="$(create_release)"
ensure_venv
sync_dependencies "${RELEASE_DIR}"
switch_release "${RELEASE_DIR}"
write_env_file
write_service_unit
initialize_database
restart_service
wait_for_health
cleanup_old_releases

echo "生产部署完成"
echo "平台: Linux systemd"
echo "监听: ${PROD_API_HOST}:${PROD_API_PORT}"
echo "本机健康检查: http://127.0.0.1:${PROD_API_PORT}/health"
echo "外部访问: http://<你的服务器公网IP>:${PROD_API_PORT}"
echo "服务名: ${PROD_SERVICE_UNIT_NAME}"
echo "systemctl status ${PROD_SERVICE_UNIT_NAME}"
echo "journalctl -u ${PROD_SERVICE_UNIT_NAME} -f"
echo "自动同步: 工作日 ${PROD_SYNC_HOUR}:$(printf '%02d' "${PROD_SYNC_MINUTE}")"
