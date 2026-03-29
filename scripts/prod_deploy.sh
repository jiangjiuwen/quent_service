#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$(uname -s)" in
    Darwin)
        exec "${SCRIPT_DIR}/prod_deploy_macos.sh" "$@"
        ;;
    Linux)
        exec "${SCRIPT_DIR}/prod_deploy_linux.sh" "$@"
        ;;
    *)
        echo "不支持的操作系统: $(uname -s)" >&2
        exit 1
        ;;
esac
