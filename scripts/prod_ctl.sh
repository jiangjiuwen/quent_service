#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/prod_paths.sh"

job_target() {
    local label="$1"
    printf '%s/%s' "${GUI_DOMAIN}" "${label}"
}

job_loaded() {
    local label="$1"
    launchctl print "$(job_target "${label}")" >/dev/null 2>&1
}

job_field() {
    local label="$1"
    local pattern="$2"
    launchctl print "$(job_target "${label}")" 2>/dev/null | awk -F'= ' -v pattern="${pattern}" '$0 ~ pattern {gsub(/^[[:space:]]+/, "", $2); print $2; exit}'
}

job_pid() {
    job_field "$1" "pid ="
}

ensure_loaded() {
    local label="$1"
    local plist="$2"

    if job_loaded "${label}"; then
        return 0
    fi

    launchctl bootstrap "${GUI_DOMAIN}" "${plist}"
}

require_deployed() {
    if [ ! -f "${PROD_SERVICE_PLIST}" ] || [ ! -f "${PROD_ENV_FILE}" ]; then
        echo "生产环境尚未部署，请先执行: scripts/prod_deploy.sh" >&2
        exit 1
    fi
}

print_job_status() {
    local title="$1"
    local label="$2"
    local state pid last_exit

    if ! job_loaded "${label}"; then
        echo "${title}: 未加载 (${label})"
        return 0
    fi

    state="$(job_field "${label}" "state =")"
    pid="$(job_pid "${label}")"
    last_exit="$(job_field "${label}" "last exit code =")"

    echo "${title}: 已加载 (${label})"
    echo "  state=${state:-unknown} pid=${pid:-none} last_exit=${last_exit:-unknown}"
}

print_health() {
    local url="http://${PROD_API_HOST}:${PROD_API_PORT}/health"
    if curl --silent --show-error --fail "${url}" >/dev/null 2>&1; then
        echo "Health: $(curl --silent --show-error "${url}")"
    else
        echo "Health: unavailable (${url})"
    fi
}

start_service() {
    require_deployed
    ensure_loaded "${PROD_SERVICE_LABEL}" "${PROD_SERVICE_PLIST}"
    launchctl kickstart -k "$(job_target "${PROD_SERVICE_LABEL}")"
}

stop_service() {
    if ! job_loaded "${PROD_SERVICE_LABEL}"; then
        echo "生产服务未加载"
        return 0
    fi

    launchctl bootout "$(job_target "${PROD_SERVICE_LABEL}")"
}

watch_on() {
    require_deployed
    ensure_loaded "${PROD_WATCH_LABEL}" "${PROD_WATCH_PLIST}"
    launchctl kickstart -k "$(job_target "${PROD_WATCH_LABEL}")"
}

watch_off() {
    if ! job_loaded "${PROD_WATCH_LABEL}"; then
        echo "自动更新监听未加载"
        return 0
    fi

    launchctl bootout "$(job_target "${PROD_WATCH_LABEL}")"
}

show_logs() {
    local target="${1:-service}"
    local lines="${2:-50}"

    case "${target}" in
        service)
            tail -n "${lines}" -f "${SERVICE_STDOUT_LOG}" "${SERVICE_STDERR_LOG}"
            ;;
        watch)
            tail -n "${lines}" -f "${WATCH_STDOUT_LOG}" "${WATCH_STDERR_LOG}"
            ;;
        all)
            tail -n "${lines}" -f \
                "${SERVICE_STDOUT_LOG}" \
                "${SERVICE_STDERR_LOG}" \
                "${WATCH_STDOUT_LOG}" \
                "${WATCH_STDERR_LOG}"
            ;;
        *)
            echo "未知日志目标: ${target}" >&2
            exit 1
            ;;
    esac
}

usage() {
    cat <<EOF
用法:
  scripts/prod_ctl.sh <command>

命令:
  status            查看 API / 自动更新监听状态
  start             启动生产 API
  stop              停止生产 API
  restart           重启生产 API
  deploy [...]      手动部署生产快照，参数透传给 prod_deploy.sh
  watch-on          开启代码变更自动部署
  watch-off         关闭代码变更自动部署
  watch-status      查看自动部署状态
  logs [target]     查看日志，target=service|watch|all
EOF
}

COMMAND="${1:-status}"
case "${COMMAND}" in
    status)
        require_deployed
        print_job_status "API" "${PROD_SERVICE_LABEL}"
        print_health
        print_job_status "Watch" "${PROD_WATCH_LABEL}"
        ;;
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        start_service
        ;;
    deploy)
        shift || true
        exec "${WORKSPACE_DIR}/scripts/prod_deploy.sh" "$@"
        ;;
    watch-on)
        watch_on
        ;;
    watch-off)
        watch_off
        ;;
    watch-status)
        require_deployed
        print_job_status "Watch" "${PROD_WATCH_LABEL}"
        ;;
    logs)
        show_logs "${2:-service}" "${3:-50}"
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
