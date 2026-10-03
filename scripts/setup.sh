#!/usr/bin/env bash
# funread 唯一的长时间运行服务：NiceGUI 视频列表开发页面
# (funread.web.page.video_list)。该服务目前仅是本地开发用的演示页面，
# 没有独立发布的生产制品，也没有安装型 CLI 可以自行 daemonize，
# 因此采用 bash-service-guide 文档中的 fallback 方案：
# 由本脚本通过 nohup 后台拉起进程，并自行管理 PID 文件。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${ROOT}/.run"
PID_FILE="${RUN_DIR}/web.pid"
LOG_FILE="${RUN_DIR}/web.log"
PORT="${FUNREAD_WEB_PORT:-8080}"
MODULE="funread.web.page.video_list"
START_GRACE_SECONDS=1

readonly ROOT RUN_DIR PID_FILE LOG_FILE PORT MODULE START_GRACE_SECONDS

usage() {
  printf 'Usage: %s <start|stop|restart|run|status>\n' "${0##*/}" >&2
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
  py="$(python_bin)"
  local -a command=("${py}" -m "${MODULE}")

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
  py="$(python_bin)"
  cd "${ROOT}"
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

  case "${action}" in
    start|stop|restart|run|status)
      (( $# == 1 )) || {
        usage
        die "${action} takes no further arguments"
      }
      "do_${action}"
      ;;
    *)
      usage
      die "unknown action: ${action:-<empty>}"
      ;;
  esac
}

main "$@"
