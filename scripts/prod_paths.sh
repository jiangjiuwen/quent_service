#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="${QUANT_WORKSPACE_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
OS_NAME="${QUANT_OS_NAME:-$(uname -s)}"

default_prod_root() {
    case "${OS_NAME}" in
        Darwin)
            printf '%s\n' "${HOME}/Library/Application Support/quant_service_prod"
            ;;
        Linux)
            printf '%s\n' "${HOME}/quant_service_prod"
            ;;
        *)
            printf '%s\n' "${HOME}/quant_service_prod"
            ;;
    esac
}

PROD_ROOT="${QUANT_PROD_ROOT:-$(default_prod_root)}"
PROD_APP_DIR="${QUANT_PROD_APP_DIR:-${PROD_ROOT}/current}"
PROD_RELEASES_DIR="${QUANT_PROD_RELEASES_DIR:-${PROD_ROOT}/releases}"
PROD_VENV_DIR="${QUANT_PROD_VENV_DIR:-${PROD_ROOT}/venv}"
PROD_DATA_DIR="${QUANT_PROD_DATA_DIR:-${PROD_ROOT}/data}"
PROD_LOG_DIR="${QUANT_PROD_LOG_DIR:-${PROD_ROOT}/logs}"
PROD_TMP_DIR="${QUANT_PROD_TMP_DIR:-${PROD_ROOT}/tmp}"
PROD_DEPLOY_LOCK_DIR="${QUANT_PROD_DEPLOY_LOCK_DIR:-${PROD_ROOT}/deploy.lock}"
PROD_ENV_FILE="${QUANT_PROD_ENV_FILE:-${PROD_ROOT}/prod.env}"

PROD_PYTHON="${QUANT_PROD_PYTHON:-}"
if [ -z "${PROD_PYTHON}" ]; then
    if command -v python3.12 >/dev/null 2>&1; then
        PROD_PYTHON="$(command -v python3.12)"
    elif command -v python3.11 >/dev/null 2>&1; then
        PROD_PYTHON="$(command -v python3.11)"
    elif [ -x "${WORKSPACE_DIR}/.venv/bin/python" ]; then
        PROD_PYTHON="${WORKSPACE_DIR}/.venv/bin/python"
    else
        PROD_PYTHON="$(command -v python3)"
    fi
fi

PROD_API_PORT="${QUANT_PROD_API_PORT:-18000}"
PROD_API_HOST="${QUANT_PROD_API_HOST:-0.0.0.0}"
PROD_BATCH_SIZE="${QUANT_PROD_BATCH_SIZE:-100}"
PROD_HISTORY_YEARS="${QUANT_PROD_HISTORY_YEARS:-15}"
PROD_START_YEAR="${QUANT_PROD_START_YEAR:-2010}"
PROD_END_YEAR="${QUANT_PROD_END_YEAR:-2025}"
PROD_LOG_LEVEL="${QUANT_PROD_LOG_LEVEL:-INFO}"
PROD_SYNC_HOUR="${QUANT_PROD_SYNC_HOUR:-15}"
PROD_SYNC_MINUTE="${QUANT_PROD_SYNC_MINUTE:-30}"

KEEP_RELEASES="${QUANT_PROD_KEEP_RELEASES:-5}"
WATCH_POLL_SECONDS="${QUANT_PROD_WATCH_POLL_SECONDS:-2}"
WATCH_DEBOUNCE_SECONDS="${QUANT_PROD_WATCH_DEBOUNCE_SECONDS:-3}"

SERVICE_STDOUT_LOG="${PROD_LOG_DIR}/service.stdout.log"
SERVICE_STDERR_LOG="${PROD_LOG_DIR}/service.stderr.log"
BACKFILL_STDOUT_LOG="${PROD_LOG_DIR}/backfill.stdout.log"
BACKFILL_STDERR_LOG="${PROD_LOG_DIR}/backfill.stderr.log"
WATCH_STDOUT_LOG="${PROD_LOG_DIR}/watch.stdout.log"
WATCH_STDERR_LOG="${PROD_LOG_DIR}/watch.stderr.log"
BACKFILL_STATE_FILE="${PROD_DATA_DIR}/backfill_state.json"
WATCH_STATE_FILE="${PROD_ROOT}/watch_state.json"

PROD_SERVICE_LABEL=""
PROD_BACKFILL_LABEL=""
PROD_WATCH_LABEL=""
LAUNCH_AGENTS_DIR=""
PROD_SERVICE_PLIST=""
PROD_BACKFILL_PLIST=""
PROD_WATCH_PLIST=""
GUI_DOMAIN=""
PROD_SERVICE_NAME=""
PROD_SERVICE_UNIT_NAME=""
SYSTEMD_UNIT_DIR=""
PROD_SERVICE_UNIT=""
PROD_SERVICE_USER=""
PROD_SERVICE_GROUP=""

case "${OS_NAME}" in
    Darwin)
        PROD_SERVICE_LABEL="${QUANT_PROD_SERVICE_LABEL:-com.jiangjiuwen.quant-service}"
        PROD_BACKFILL_LABEL="${QUANT_PROD_BACKFILL_LABEL:-com.jiangjiuwen.quant-service.backfill}"
        PROD_WATCH_LABEL="${QUANT_PROD_WATCH_LABEL:-com.jiangjiuwen.quant-service.watch}"
        LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
        PROD_SERVICE_PLIST="${LAUNCH_AGENTS_DIR}/${PROD_SERVICE_LABEL}.plist"
        PROD_BACKFILL_PLIST="${LAUNCH_AGENTS_DIR}/${PROD_BACKFILL_LABEL}.plist"
        PROD_WATCH_PLIST="${LAUNCH_AGENTS_DIR}/${PROD_WATCH_LABEL}.plist"
        GUI_DOMAIN="gui/$(id -u)"
        ;;
    Linux)
        PROD_SERVICE_NAME="${QUANT_PROD_SERVICE_NAME:-quant-service}"
        PROD_SERVICE_UNIT_NAME="${PROD_SERVICE_NAME%.service}.service"
        SYSTEMD_UNIT_DIR="${QUANT_PROD_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
        PROD_SERVICE_UNIT="${SYSTEMD_UNIT_DIR}/${PROD_SERVICE_UNIT_NAME}"
        PROD_SERVICE_USER="${QUANT_PROD_SERVICE_USER:-$(id -un)}"
        if [ -n "${QUANT_PROD_SERVICE_GROUP:-}" ]; then
            PROD_SERVICE_GROUP="${QUANT_PROD_SERVICE_GROUP}"
        else
            PROD_SERVICE_GROUP="$(id -gn "${PROD_SERVICE_USER}" 2>/dev/null || printf '%s' "${PROD_SERVICE_USER}")"
        fi
        ;;
    *)
        echo "不支持的操作系统: ${OS_NAME}" >&2
        exit 1
        ;;
esac

hash_file_sha256() {
    local file_path="$1"
    "${PROD_PYTHON}" - "${file_path}" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
PY
}

copy_workspace_to_release() {
    local release_dir="$1"

    if command -v rsync >/dev/null 2>&1; then
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
        return 0
    fi

    (
        cd "${WORKSPACE_DIR}"
        tar \
            --exclude='.venv' \
            --exclude='data' \
            --exclude='logs' \
            --exclude='__pycache__' \
            --exclude='*.pyc' \
            --exclude='*.pyo' \
            --exclude='.DS_Store' \
            --exclude='.pytest_cache' \
            --exclude='.mypy_cache' \
            --exclude='.ruff_cache' \
            --exclude='.git' \
            -cf - .
    ) | (
        cd "${release_dir}"
        tar -xf -
    )
}

load_prod_env() {
    set -a
    # shellcheck disable=SC1090
    source "${PROD_ENV_FILE}"
    set +a
}

export OS_NAME WORKSPACE_DIR PROD_ROOT PROD_APP_DIR PROD_RELEASES_DIR PROD_VENV_DIR PROD_DATA_DIR PROD_LOG_DIR
export PROD_TMP_DIR PROD_DEPLOY_LOCK_DIR PROD_ENV_FILE
export PROD_PYTHON
export PROD_API_PORT PROD_API_HOST PROD_BATCH_SIZE PROD_HISTORY_YEARS PROD_START_YEAR PROD_END_YEAR PROD_LOG_LEVEL
export PROD_SYNC_HOUR PROD_SYNC_MINUTE
export KEEP_RELEASES WATCH_POLL_SECONDS WATCH_DEBOUNCE_SECONDS
export SERVICE_STDOUT_LOG SERVICE_STDERR_LOG BACKFILL_STDOUT_LOG BACKFILL_STDERR_LOG
export WATCH_STDOUT_LOG WATCH_STDERR_LOG BACKFILL_STATE_FILE WATCH_STATE_FILE
export PROD_SERVICE_LABEL PROD_BACKFILL_LABEL PROD_WATCH_LABEL
export LAUNCH_AGENTS_DIR PROD_SERVICE_PLIST PROD_BACKFILL_PLIST PROD_WATCH_PLIST GUI_DOMAIN
export PROD_SERVICE_NAME PROD_SERVICE_UNIT_NAME SYSTEMD_UNIT_DIR PROD_SERVICE_UNIT PROD_SERVICE_USER PROD_SERVICE_GROUP
