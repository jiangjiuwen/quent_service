#!/bin/bash

set -euo pipefail

usage() {
    cat <<EOF
Usage:
  scripts/bootstrap_tencent_linux.sh [options]

Options:
  --user USER             Linux deploy user. Defaults to the sudo caller.
  --workspace PATH        GitHub Actions sync workspace path.
  --prod-root PATH        Production root path.
  --port PORT             API port. Default: 18000.
  --service-name NAME     systemd service name. Default: quant-service.
  -h, --help              Show this help message.

Supported distributions:
  - OpenCloudOS / RHEL / CentOS / Rocky / AlmaLinux / Oracle Linux
  - Ubuntu / Debian

Example:
  sudo bash scripts/bootstrap_tencent_linux.sh --user deploy
EOF
}

log() {
    printf '[bootstrap] %s\n' "$*"
}

require_root() {
    if [ "$(id -u)" -eq 0 ]; then
        return 0
    fi

    if ! command -v sudo >/dev/null 2>&1; then
        echo "This script must run as root or with sudo." >&2
        exit 1
    fi

    exec sudo -E bash "$0" "$@"
}

resolve_user_home() {
    local user_name="$1"
    local home_dir

    home_dir="$(getent passwd "${user_name}" | cut -d: -f6)"
    if [ -z "${home_dir}" ]; then
        echo "Unable to resolve home directory for user: ${user_name}" >&2
        exit 1
    fi

    printf '%s\n' "${home_dir}"
}

detect_group() {
    local user_name="$1"
    id -gn "${user_name}" 2>/dev/null || printf '%s\n' "${user_name}"
}

load_os_release() {
    if [ -f /etc/os-release ]; then
        # shellcheck disable=SC1091
        source /etc/os-release
    else
        echo "Unsupported Linux distribution: missing /etc/os-release" >&2
        exit 1
    fi

    OS_ID="${ID:-unknown}"
    OS_ID_LIKE="${ID_LIKE:-}"
}

is_deb_family() {
    case "${OS_ID}" in
        ubuntu|debian)
            return 0
            ;;
    esac

    case " ${OS_ID_LIKE} " in
        *" debian "*|*" ubuntu "*)
            return 0
            ;;
    esac

    return 1
}

is_rpm_family() {
    case "${OS_ID}" in
        opencloudos|opencloudos-stream|rhel|centos|rocky|almalinux|ol|fedora|anolis|alinux)
            return 0
            ;;
    esac

    case " ${OS_ID_LIKE} " in
        *" rhel "*|*" centos "*|*" fedora "*)
            return 0
            ;;
    esac

    return 1
}

package_manager_cmd() {
    if command -v dnf >/dev/null 2>&1; then
        printf '%s\n' "dnf"
        return 0
    fi

    if command -v yum >/dev/null 2>&1; then
        printf '%s\n' "yum"
        return 0
    fi

    echo "Unable to find dnf or yum on this RPM-based system." >&2
    exit 1
}

install_packages() {
    if is_deb_family; then
        log "Detected Debian family: ${OS_ID}"
        apt-get update
        DEBIAN_FRONTEND=noninteractive apt-get install -y \
            curl \
            git \
            python3 \
            python3-pip \
            python3-venv \
            rsync \
            sqlite3 \
            sudo \
            zstd
        return 0
    fi

    if is_rpm_family; then
        local pm
        pm="$(package_manager_cmd)"
        log "Detected RPM family: ${OS_ID} via ${pm}"
        "${pm}" install -y \
            curl \
            git \
            python3 \
            python3-pip \
            rsync \
            sqlite \
            sudo \
            zstd
        return 0
    fi

    echo "Unsupported Linux distribution: ID=${OS_ID}, ID_LIKE=${OS_ID_LIKE}" >&2
    exit 1
}

write_sudoers() {
    local deploy_user="$1"
    local sudoers_file="/etc/sudoers.d/quant-service-deploy"
    local temp_file

    temp_file="$(mktemp)"
    cat > "${temp_file}" <<EOF
${deploy_user} ALL=(ALL) NOPASSWD:/usr/bin/systemctl,/bin/systemctl,/usr/bin/install,/bin/install,/usr/bin/journalctl,/bin/journalctl
EOF

    if command -v visudo >/dev/null 2>&1; then
        visudo -cf "${temp_file}" >/dev/null
    fi

    install -d -m 0755 /etc/sudoers.d
    install -m 0440 "${temp_file}" "${sudoers_file}"
    rm -f "${temp_file}"
}

verify_python_venv() {
    python3 - <<'PY'
import sys
import venv

print(f"python ok: {sys.version.split()[0]}")
PY
}

show_next_steps() {
    local deploy_user="$1"
    local workspace_dir="$2"
    local prod_root="$3"
    local api_port="$4"
    local service_name="$5"

    cat <<EOF

Bootstrap completed.

GitHub repository variables:
TENCENT_CVM_HOST=<your-server-public-ip>
TENCENT_CVM_PORT=22
TENCENT_CVM_USER=${deploy_user}
TENCENT_CVM_WORKSPACE=${workspace_dir}
QUANT_PROD_ROOT=${prod_root}
QUANT_PROD_API_PORT=${api_port}
QUANT_PROD_SERVICE_NAME=${service_name}
QUANT_PROD_SERVICE_USER=${deploy_user}

GitHub repository secret:
TENCENT_CVM_SSH_KEY=<paste-private-key-content>

Recommended SSH setup:
1. Generate a dedicated deploy key locally:
   ssh-keygen -t ed25519 -f ~/.ssh/tencent_quant_deploy -C "github-actions-deploy"
2. Add these entries to your local ~/.ssh/config:
   Host root
     HostName <your-server-public-ip>
     User root
     Port 22

   Host deploy
     HostName <your-server-public-ip>
     User ${deploy_user}
     Port 22
     IdentityFile ~/.ssh/tencent_quant_deploy
     IdentitiesOnly yes
3. Install the public key into /home/${deploy_user}/.ssh/authorized_keys on the server.
4. For all later SSH / rsync commands, use:
   ssh root
   ssh deploy
   rsync ... deploy:/home/${deploy_user}/
5. Copy the private key content into GitHub secret TENCENT_CVM_SSH_KEY:
   cat ~/.ssh/tencent_quant_deploy
EOF
}

DEPLOY_USER="${SUDO_USER:-}"
WORKSPACE_DIR=""
PROD_ROOT=""
API_PORT="18000"
SERVICE_NAME="quant-service"

while [ $# -gt 0 ]; do
    case "$1" in
        --user)
            DEPLOY_USER="${2:-}"
            shift 2
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

require_root "$@"

if [ -z "${DEPLOY_USER}" ] || [ "${DEPLOY_USER}" = "root" ]; then
    echo "Please provide a non-root deploy user with --user." >&2
    exit 1
fi

if ! getent passwd "${DEPLOY_USER}" >/dev/null 2>&1; then
    echo "User does not exist: ${DEPLOY_USER}" >&2
    exit 1
fi

load_os_release

DEPLOY_HOME="$(resolve_user_home "${DEPLOY_USER}")"
DEPLOY_GROUP="$(detect_group "${DEPLOY_USER}")"

if [ -z "${WORKSPACE_DIR}" ]; then
    WORKSPACE_DIR="${DEPLOY_HOME}/quent_service"
fi

if [ -z "${PROD_ROOT}" ]; then
    PROD_ROOT="${DEPLOY_HOME}/quant_service_prod"
fi

log "Installing required packages"
install_packages

log "Verifying python3 venv support"
verify_python_venv

log "Creating workspace and production directories"
install -d -o "${DEPLOY_USER}" -g "${DEPLOY_GROUP}" -m 0755 \
    "${WORKSPACE_DIR}" \
    "${PROD_ROOT}" \
    "${PROD_ROOT}/releases" \
    "${PROD_ROOT}/data" \
    "${PROD_ROOT}/logs" \
    "${PROD_ROOT}/tmp"

log "Writing sudoers policy for deployment"
write_sudoers "${DEPLOY_USER}"

log "Validating non-interactive sudo for deployment commands"
sudo -u "${DEPLOY_USER}" sudo -n systemctl --version >/dev/null
sudo -u "${DEPLOY_USER}" sudo -n install --version >/dev/null

show_next_steps "${DEPLOY_USER}" "${WORKSPACE_DIR}" "${PROD_ROOT}" "${API_PORT}" "${SERVICE_NAME}"
