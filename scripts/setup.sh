#!/usr/bin/env bash
set -euo pipefail

# funread 是 package 类应用：只会被 import / pip install 成依赖，
# 没有任何东西把它当长驻进程启动。因此它只实现 release action，
# 不实现 start/stop/restart/run/status，也没有 `server` 子命令 ——
# 对它执行 service action 是用法错误，这里显式报错而不是静默跳过。

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

PKG_NAME="funread"
PYTHON="${PYTHON:-python3}"

readonly ROOT PKG_NAME PYTHON

usage() {
  printf 'Usage: %s install-dev\n' "${0##*/}" >&2
  printf '       %s <install-prod|upgrade> [version]\n' "${0##*/}" >&2
  printf '       %s rollback <version>\n' "${0##*/}" >&2
  printf '       %s uninstall\n' "${0##*/}" >&2
  printf '\n' >&2
  printf '%s 是 package 类应用，不支持 start/stop/restart/run/status。\n' "${PKG_NAME}" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 2
}

# 清掉上一次的构建产物与本地安装，从工作树重新构建并强制重装。
do_install_dev() {
  rm -rf dist build
  funbuild install
}

do_install_prod() {
  local version="${1:-}"
  "${PYTHON}" -m pip install "${PKG_NAME}${version:+==${version}}"
}

# funread 没有自己的 CLI（pyproject 里没有 [project.scripts]），
# 所以 upgrade/rollback 直接退回到包管理器，而不是转发给某个 CLI 子命令。
do_upgrade() {
  local version="${1:-}"
  if [[ -n "${version}" ]]; then
    "${PYTHON}" -m pip install "${PKG_NAME}==${version}"
  else
    "${PYTHON}" -m pip install --upgrade "${PKG_NAME}"
  fi
}

do_rollback() {
  local version="$1"
  "${PYTHON}" -m pip install "${PKG_NAME}==${version}"
}

do_uninstall() {
  "${PYTHON}" -m pip uninstall -y "${PKG_NAME}"
}

main() {
  local action="${1:-}"

  case "${action}" in
    start | stop | restart | run | status)
      usage
      die "${action} 只适用于 service 类应用；${PKG_NAME} 是 package 类应用"
      ;;
    install-dev | uninstall)
      (( $# == 1 )) || {
        usage
        die "${action} 不接受额外参数"
      }
      "do_${action//-/_}"
      ;;
    install-prod | upgrade)
      (( $# <= 2 )) || {
        usage
        die "${action} 最多接受一个 version 参数"
      }
      "do_${action//-/_}" "${2:-}"
      ;;
    rollback)
      (( $# == 2 )) || {
        usage
        die "rollback 必须显式给出版本号"
      }
      do_rollback "$2"
      ;;
    *)
      usage
      die "unknown action: ${action:-<empty>}"
      ;;
  esac
}

main "$@"
