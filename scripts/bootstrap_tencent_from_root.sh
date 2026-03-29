#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
    cat <<EOF
Usage:
  scripts/bootstrap_tencent_from_root.sh [options]

Options:
  --user USER             Deploy user to create or reuse.
  --create-user           Create the deploy user if it does not exist.
  --workspace PATH        Workspace path on the server.
  --prod-root PATH        Production root path.
  --port PORT             API port. Default: 18000.
  --service-name NAME     systemd service name. Default: quant-service.
  --repo-dir PATH         Source repository directory. Default: current repo.
  --skip-seed             Do not copy the current repository into the workspace.
  -h, --help              Show this help message.

Example:
  sudo bash scripts/bootstrap_tencent_from_root.sh --user deploy --create-user
EOF
}

log() {
    printf '[root-bootstrap] %s\n' "$*"
}

require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "This script must be run as root." >&2
        exit 1
    fi
}

resolve_user_home() {
    local user_name="$1"
    getent passwd "${user_name}" | cut -d: -f6
}

detect_group() {
    local user_name="$1"
    id -gn "${user_name}" 2>/dev/null || printf '%s\n' "${user_name}"
}

grant_admin_group() {
    local user_name="$1"

    if getent group sudo >/dev/null 2>&1; then
        usermod -aG sudo "${user_name}" >/dev/null 2>&1 || true
    fi

    if getent group wheel >/dev/null 2>&1; then
        usermod -aG wheel "${user_name}" >/dev/null 2>&1 || true
    fi
}

ensure_user() {
    local user_name="$1"
    local should_create="$2"

    if id -u "${user_name}" >/dev/null 2>&1; then
        grant_admin_group "${user_name}"
        return 0
    fi

    if [ "${should_create}" != "true" ]; then
        echo "User does not exist: ${user_name}. Re-run with --create-user to create it." >&2
        exit 1
    fi

    log "Creating deploy user ${user_name}"
    useradd -m -s /bin/bash "${user_name}"
    grant_admin_group "${user_name}"
}

copy_repo_to_workspace() {
    local source_dir="$1"
    local target_dir="$2"
    local target_user="$3"
    local target_group="$4"
    local source_real target_real

    source_real="$(cd "${source_dir}" && pwd)"
    target_real="$(mkdir -p "${target_dir}" && cd "${target_dir}" && pwd)"

    if [ "${source_real}" = "${target_real}" ]; then
        log "Workspace already points at the current repository, skipping seed"
        return 0
    fi

    log "Seeding repository into ${target_dir}"
    if command -v rsync >/dev/null 2>&1; then
        rsync -a --delete \
            --exclude '.git/' \
            --exclude '.github/' \
            --exclude '.venv/' \
            --exclude 'data/' \
            --exclude 'logs/' \
            --exclude '__pycache__/' \
            --exclude '.pytest_cache/' \
            --exclude '.mypy_cache/' \
            --exclude '.ruff_cache/' \
            --exclude '.DS_Store' \
            "${source_dir}/" "${target_dir}/"
    else
        (
            cd "${source_dir}"
            tar \
                --exclude='.git' \
                --exclude='.github' \
                --exclude='.venv' \
                --exclude='data' \
                --exclude='logs' \
                --exclude='__pycache__' \
                --exclude='.pytest_cache' \
                --exclude='.mypy_cache' \
                --exclude='.ruff_cache' \
                --exclude='.DS_Store' \
                -cf - .
        ) | (
            cd "${target_dir}"
            tar -xf -
        )
    fi

    chown -R "${target_user}:${target_group}" "${target_dir}"
}

show_next_steps() {
    local deploy_user="$1"
    local workspace_dir="$2"

    cat <<EOF

Root bootstrap completed.

Next recommended commands:
1. Test a manual deployment once:
   sudo -u ${deploy_user} bash -lc 'cd ${workspace_dir} && ./scripts/prod_deploy.sh'
2. Configure GitHub Actions variables from:
   docs/tencent-cloud-actions-vars.example
3. Push to main and let GitHub Actions perform subsequent deployments.
EOF
}

DEPLOY_USER=""
CREATE_USER="false"
WORKSPACE_DIR=""
PROD_ROOT=""
API_PORT="18000"
SERVICE_NAME="quant-service"
REPO_DIR="${REPO_ROOT}"
SKIP_SEED="false"

while [ $# -gt 0 ]; do
    case "$1" in
        --user)
            DEPLOY_USER="${2:-}"
            shift 2
            ;;
        --create-user)
            CREATE_USER="true"
            shift
            ;;
        --workspace)
            WORKSPACE_DIR="${2:-}"
            shift 2
            ;;
        --prod-root)
            PROD_ROOT="${2:-}"
            shift 2
            ;;
        --port)
            API_PORT="${2:-}"
            shift 2
            ;;
        --service-name)
            SERVICE_NAME="${2:-}"
            shift 2
            ;;
        --repo-dir)
            REPO_DIR="${2:-}"
            shift 2
            ;;
        --skip-seed)
            SKIP_SEED="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

require_root

if [ -z "${DEPLOY_USER}" ] || [ "${DEPLOY_USER}" = "root" ]; then
    echo "Please provide a non-root deploy user with --user." >&2
    exit 1
fi

if [ ! -d "${REPO_DIR}" ] || [ ! -f "${REPO_DIR}/scripts/prod_deploy.sh" ]; then
    echo "Invalid repo directory: ${REPO_DIR}" >&2
    exit 1
fi

ensure_user "${DEPLOY_USER}" "${CREATE_USER}"

DEPLOY_HOME="$(resolve_user_home "${DEPLOY_USER}")"
DEPLOY_GROUP="$(detect_group "${DEPLOY_USER}")"

if [ -z "${WORKSPACE_DIR}" ]; then
    WORKSPACE_DIR="${DEPLOY_HOME}/quent_service"
fi

if [ -z "${PROD_ROOT}" ]; then
    PROD_ROOT="${DEPLOY_HOME}/quant_service_prod"
fi

log "Running Linux bootstrap for ${DEPLOY_USER}"
bash "${SCRIPT_DIR}/bootstrap_tencent_linux.sh" \
    --user "${DEPLOY_USER}" \
    --workspace "${WORKSPACE_DIR}" \
    --prod-root "${PROD_ROOT}" \
    --port "${API_PORT}" \
    --service-name "${SERVICE_NAME}"

if [ "${SKIP_SEED}" != "true" ]; then
    copy_repo_to_workspace "${REPO_DIR}" "${WORKSPACE_DIR}" "${DEPLOY_USER}" "${DEPLOY_GROUP}"
fi

show_next_steps "${DEPLOY_USER}" "${WORKSPACE_DIR}"
