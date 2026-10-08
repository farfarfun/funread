#!/usr/bin/env bash
# funread 唯一的长时间运行服务：NiceGUI 视频列表页面
# (funread.web.page.video_list)。由本脚本通过 nohup 后台拉起进程，并自行
# 管理 PID 文件。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULE="funread.web.page.video_list"
START_GRACE_SECONDS=1

readonly ROOT MODULE START_GRACE_SECONDS

usage() {
  printf 'Usage: %s <start|stop|restart|run|status> <dev|prod>\n' "${0##*/}" >&2
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 2
}

python_bin() {
  if [[ -x "${ROOT}/.venv/bin/python" ]]; then
    printf '%s\n' "${ROOT}/.venv/bin/python"
  else
    command -v python3 || command -v python || die "python interpreter not found"
  fi
}

configure_environment() {
  ENVIRONMENT="$1"
  RUN_DIR="${ROOT}/.run/${ENVIRONMENT}"
  PID_FILE="${RUN_DIR}/web.pid"
  LOG_FILE="${RUN_DIR}/web.log"

  case "${ENVIRONMENT}" in
    dev)
      PORT="${FUNREAD_WEB_PORT_DEV:-${FUNREAD_WEB_PORT:-8080}}"
      WORK_DIR="${ROOT}"
      ;;
    prod)
      PORT="${FUNREAD_WEB_PORT_PROD:-${FUNREAD_WEB_PORT:-8080}}"
      # Do not leave the repository on sys.path when serving an installed package.
      WORK_DIR="/"
      ;;
    *)
      usage
      die "environment must be dev or prod"
      ;;
  esac
}

assert_production_package() {
  local py="$1"
  if ! (
    cd / && "${py}" -c '
from importlib.metadata import version
import os
import sys
import funread
import importlib

root = os.path.realpath(sys.argv[1])
origin = os.path.realpath(funread.__file__ or "")
version("funread")
if not origin or os.path.commonpath((root, origin)) == root:
    raise SystemExit("funread must be installed as a non-editable package")
importlib.import_module("funread.web.page.video_list")
' "${ROOT}"
  ); then
    die "prod requires an installed, non-editable funread package with the web extra"
  fi
}

runtime_python() {
  local py
  py="$(python_bin)"
  if [[ "${ENVIRONMENT}" == "prod" ]]; then
    assert_production_package "${py}"
  fi
  printf '%s\n' "${py}"
}

is_pid_alive() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

read_pid() {
  [[ -f "${PID_FILE}" ]] || return 1
  local pid
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  [[ "${pid}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${pid}"
}

do_start() {
  mkdir -p "${RUN_DIR}"
  local existing_pid
  if existing_pid="$(read_pid)" && is_pid_alive "${existing_pid}"; then
    die "web is already running (pid ${existing_pid})"
  fi

  local py
  py="$(runtime_python)"
  local -a command=("${py}" -m "${MODULE}")

  cd "${WORK_DIR}"
  FUNREAD_WEB_PORT="${PORT}" nohup "${command[@]}" </dev/null >>"${LOG_FILE}" 2>&1 &
  local pid=$!
  echo "${pid}" > "${PID_FILE}"

  sleep "${START_GRACE_SECONDS}"
  if ! is_pid_alive "${pid}"; then
    rm -f "${PID_FILE}"
    die "web failed to start, see ${LOG_FILE}"
  fi
  printf 'web started (pid %s, port %s), log: %s\n' "${pid}" "${PORT}" "${LOG_FILE}"
}

do_run() {
  local py
  py="$(runtime_python)"
  cd "${WORK_DIR}"
  FUNREAD_WEB_PORT="${PORT}" exec "${py}" -m "${MODULE}"
}

do_stop() {
  local pid
  if ! pid="$(read_pid)"; then
    printf 'web is not running (no pid file)\n'
    return 0
  fi
  if ! is_pid_alive "${pid}"; then
    printf 'web is not running (stale pid file), cleaning up\n'
    rm -f "${PID_FILE}"
    return 0
  fi

  kill -TERM "${pid}" 2>/dev/null || true
  local waited=0
  while is_pid_alive "${pid}" && (( waited < 10 )); do
    sleep 1
    waited=$((waited + 1))
  done
  if is_pid_alive "${pid}"; then
    kill -KILL "${pid}" 2>/dev/null || true
  fi
  rm -f "${PID_FILE}"
  printf 'web stopped (pid %s)\n' "${pid}"
}

do_restart() {
  do_stop
  do_start
}

do_status() {
  local pid
  if ! pid="$(read_pid)" || ! is_pid_alive "${pid}"; then
    printf 'web: stopped\n'
    return 1
  fi
  printf 'web: running (pid %s, port %s)\n' "${pid}" "${PORT}"
}

main() {
  local action="${1:-}"
  local environment="${2:-}"

  case "${action}" in
    start|stop|restart|run|status)
      (( $# == 2 )) || {
        usage
        die "${action} requires exactly one environment argument"
      }
      configure_environment "${environment}"
      "do_${action}"
      ;;
    *)
      usage
      die "unknown action: ${action:-<empty>}"
      ;;
  esac
}

main "$@"
