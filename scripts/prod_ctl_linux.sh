#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

if [ "${OS_NAME}" != "Linux" ]; then
    echo "prod_ctl_linux.sh 仅支持 Linux" >&2
    exit 1
fi

ensure_systemd() {
    if ! command -v systemctl >/dev/null 2>&1; then
        echo "未找到 systemctl，当前 Linux 环境无法使用 systemd 控制服务" >&2
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

    echo "该命令需要 root 权限，请使用 root 执行或安装 sudo" >&2
    exit 1
}

systemctl_cmd() {
    ensure_systemd
    run_as_root systemctl "$@"
}

systemctl_readonly_cmd() {
    ensure_systemd
    systemctl "$@"
}

journalctl_cmd() {
    if [ "$(id -u)" -eq 0 ]; then
        journalctl "$@"
        return 0
    fi

    if command -v sudo >/dev/null 2>&1; then
        sudo journalctl "$@"
        return 0
    fi

    journalctl "$@"
}

require_deployed() {
    if [ ! -L "${PROD_APP_DIR}" ] || [ ! -f "${PROD_ENV_FILE}" ] || [ ! -f "${PROD_SERVICE_UNIT}" ]; then
        echo "生产环境尚未部署，请先执行: scripts/prod_deploy.sh" >&2
        exit 1
    fi
}

service_field() {
    local field="$1"
    systemctl_readonly_cmd show -p "${field}" --value "${PROD_SERVICE_UNIT_NAME}" 2>/dev/null || true
}

print_status() {
    local active sub_state enabled pid
    active="$(service_field ActiveState)"
    sub_state="$(service_field SubState)"
    enabled="$(service_field UnitFileState)"
    pid="$(service_field MainPID)"

    echo "API: ${active:-unknown}/${sub_state:-unknown} enabled=${enabled:-unknown} pid=${pid:-none} service=${PROD_SERVICE_UNIT_NAME}"
}

print_health() {
    local api_host="${PROD_API_HOST}"
    local api_port="${PROD_API_PORT}"
    local url

    if [ -f "${PROD_ENV_FILE}" ]; then
        load_prod_env
        api_host="${QUANT_API_HOST:-${api_host}}"
        api_port="${QUANT_API_PORT:-${api_port}}"
    fi

    url="http://${api_host}:${api_port}/health"
    if curl --silent --show-error --fail "${url}" >/dev/null 2>&1; then
        echo "Health: $(curl --silent --show-error "${url}")"
    else
        echo "Health: unavailable (${url})"
    fi
}

start_service() {
    require_deployed
    systemctl_cmd start "${PROD_SERVICE_UNIT_NAME}"
}

stop_service() {
    require_deployed
    systemctl_cmd stop "${PROD_SERVICE_UNIT_NAME}"
}

restart_service() {
    require_deployed
    systemctl_cmd restart "${PROD_SERVICE_UNIT_NAME}"
}

show_logs() {
    local lines="${1:-50}"
    require_deployed
    journalctl_cmd -u "${PROD_SERVICE_UNIT_NAME}" -n "${lines}" -f
}

unsupported_watch() {
    echo "Linux 版未提供代码变更自动部署监听。服务器更新代码后执行 ./scripts/prod_ctl.sh deploy 即可。" >&2
    exit 1
}

usage() {
    cat <<EOF
用法:
  scripts/prod_ctl.sh <command>

命令:
  status            查看 API 服务状态
  start             启动生产 API
  stop              停止生产 API
  restart           重启生产 API
  deploy [...]      手动部署生产快照，参数透传给 prod_deploy.sh
  logs [lines]      查看 systemd 日志并持续跟随，默认 50 行
  watch-on          Linux 不支持，返回提示
  watch-off         Linux 不支持，返回提示
  watch-status      Linux 不支持，返回提示
EOF
}

COMMAND="${1:-status}"
case "${COMMAND}" in
    status)
        require_deployed
        print_status
        print_health
        ;;
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        restart_service
        ;;
    deploy)
        shift || true
        exec "${WORKSPACE_DIR}/scripts/prod_deploy.sh" "$@"
        ;;
    logs)
        show_logs "${2:-50}"
        ;;
    watch-on|watch-off|watch-status)
        unsupported_watch
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "未知命令: ${COMMAND}" >&2
        usage >&2
        exit 1
        ;;
esac
