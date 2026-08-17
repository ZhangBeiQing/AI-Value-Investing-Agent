#!/usr/bin/env bash
# 每晚定时触发 daily-data-preparation skill 的 cron 包装脚本。
#
# 默认用 opencode + deepseek-v4-flash 跑（比 claude 便宜很多）。
# 需要临时换回 claude 时：RUNTIME=claude scripts/cron_daily_data_prep.sh
# 需要换模型时：      OPENCODE_MODEL=deepseek/deepseek-v4-pro scripts/cron_daily_data_prep.sh
#
# 为什么需要这层 wrapper 而不是直接把命令写进 crontab：
#   1. cron 的环境极简，没有 nvm 注入的 PATH，直接调 opencode 会 command not found；
#      即便用绝对路径，它也需要 HOME 才能找到凭证（~/.local/share/opencode/auth.json）。
#   2. crontab 里的 `%` 需要转义，日志文件名带日期会很难写。
#   3. 非交易日先本地判断并直接退出，避免白白拉起一个 agent 跑空。
#
# 手动测试：
#   scripts/cron_daily_data_prep.sh            # 用今天
#   scripts/cron_daily_data_prep.sh 2026-08-14 # 指定交易日补跑
#
# crontab 安装方式见文件末尾注释。

set -euo pipefail

REPO_ROOT="/home/zhangbeiqing/programer/AI-Value-Investing-Agent"
VENV="/home/zhangbeiqing/venv/ai_stock"
NODE_BIN="/home/zhangbeiqing/.nvm/versions/node/v24.14.0/bin"

RUNTIME="${RUNTIME:-opencode}"
OPENCODE_BIN="${NODE_BIN}/opencode"
CLAUDE_BIN="${NODE_BIN}/claude"

# 显式钉住模型：~/.config/opencode/opencode.json 里没有顶层 "model" 键，
# 不钉的话默认模型由 opencode 自己决定，可能落到贵得多的 gpt-5.6 上。
OPENCODE_MODEL="${OPENCODE_MODEL:-deepseek/deepseek-v4-flash}"

# cron 只给一个极简 PATH，这里显式补齐 node（opencode/claude 都依赖）与常规系统路径。
export PATH="${NODE_BIN}:${VENV}/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export HOME="${HOME:-/home/zhangbeiqing}"

# 单次运行的整体上限，防止卡死的任务一直挂着占资源（2.5 小时）。
RUN_TIMEOUT_SECONDS="${RUN_TIMEOUT_SECONDS:-9000}"

cd "${REPO_ROOT}"

RUN_DATE="${1:-$(date +%F)}"
LOG_DIR="${REPO_ROOT}/logs/cron_daily_prep"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/${RUN_DATE}.log"
STATUS_FILE="${LOG_DIR}/latest_status.json"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >>"${LOG_FILE}"
}

write_status() {
    # $1=status $2=message
    cat >"${STATUS_FILE}" <<EOF
{
  "run_date": "${RUN_DATE}",
  "runtime": "${RUNTIME}",
  "model": "${OPENCODE_MODEL}",
  "status": "$1",
  "message": "$2",
  "finished_at": "$(date '+%Y-%m-%d %H:%M:%S')",
  "log_file": "${LOG_FILE}"
}
EOF
}

log "=== cron 触发 daily-data-preparation, run_date=${RUN_DATE}, runtime=${RUNTIME} ==="

case "${RUNTIME}" in
    opencode)
        if [[ ! -x "${OPENCODE_BIN}" ]]; then
            log "FATAL: opencode 不存在或不可执行: ${OPENCODE_BIN}（node 升级后路径会变，需同步本脚本）"
            write_status "failed" "opencode binary missing"
            exit 1
        fi
        ;;
    claude)
        if [[ ! -x "${CLAUDE_BIN}" ]]; then
            log "FATAL: claude 不存在或不可执行: ${CLAUDE_BIN}"
            write_status "failed" "claude binary missing"
            exit 1
        fi
        ;;
    *)
        log "FATAL: 未知 RUNTIME=${RUNTIME}，只支持 opencode / claude"
        write_status "failed" "unknown runtime"
        exit 1
        ;;
esac

# --- 交易日门禁：非交易日直接退出，不拉起 agent ---
# 复用仓库自己的交易日历，holiday 也能正确识别（crontab 的 1-5 只能排除周末）。
if ! TRADING_CHECK="$("${VENV}/bin/python" - "${RUN_DATE}" <<'PY'
import sys
from shared_data_access.market_calendar import inspect_market_session

session = inspect_market_session(sys.argv[1], market="CN", base_dir="data")
print("TRADING" if session.is_trading_day else f"NON_TRADING previous={session.previous_trading_day}")
PY
)"; then
    log "FATAL: 交易日判断失败，放弃本轮（不猜，交由人工排查）"
    write_status "failed" "trading day check crashed"
    exit 1
fi

if [[ "${TRADING_CHECK}" != TRADING* ]]; then
    log "SKIP: ${RUN_DATE} 不是 A 股交易日（${TRADING_CHECK}），本轮不运行。"
    write_status "skipped" "not a trading day"
    exit 0
fi

# --- 收盘门禁：RUN_DATE 是今天时，必须已经收盘 ---
# 只判断"是不是交易日"是不够的：周一凌晨手动跑一下，日期是交易日但根本还没开盘，
# 整条链路会基于不存在的收盘数据产出垃圾。A 股 15:00 收盘，这里留到 16:00 以给
# akshare 的当日行情/新闻一点入库时间（正常 cron 在 21:03 触发，不会碰到这道门）。
CLOSE_GUARD_HOUR="${CLOSE_GUARD_HOUR:-16}"
if [[ "${RUN_DATE}" == "$(date +%F)" ]]; then
    CURRENT_HOUR="$(date +%-H)"
    if (( CURRENT_HOUR < CLOSE_GUARD_HOUR )); then
        log "SKIP: ${RUN_DATE} 尚未收盘（当前 $(date '+%H:%M')，门禁 ${CLOSE_GUARD_HOUR}:00），本轮不运行。"
        log "      若要补跑历史交易日，请显式传日期：scripts/cron_daily_data_prep.sh YYYY-MM-DD"
        write_status "skipped" "market not closed yet"
        exit 0
    fi
fi

log "交易日确认通过，开始无头运行 ${RUNTIME}（model=${OPENCODE_MODEL}）。"

# skill 的日期默认就是 today，但这里显式把日期写进 prompt：
# 本轮耗时 50-90 分钟，万一跨过午夜，显式日期能避免中途换天。
PROMPT="开始 ${RUN_DATE} 的数据准备。严格按 daily-data-preparation skill 执行，cur_date=${RUN_DATE}。
这是 cron 无人值守运行，没有人可以实时回答你的问题：
- 遇到需要确认的地方，按 skill 的默认路径继续，把待确认事项写进最终报告，不要停下来等人。
- 不要触发任何交易 skill，不要生成或修改 05_decision.json。
- 财报总结（financial-report-summary）必须由你这个主 agent 亲自执行，不要整体丢给一个 subagent，
  否则它内部的逐股角色派发会失去控制。
- 五个阶段全部跑完后，输出一份包含各阶段状态、实际耗时和验收清单结果的最终报告。"

set +e
if [[ "${RUNTIME}" == "opencode" ]]; then
    # opencode 的 bash 工具没有后台参数，长命令靠大 timeout 撑（全局 AGENTS.md 已要求 >= 1 小时）。
    # --auto 自动批准权限，等价于 claude 的 --dangerously-skip-permissions。
    timeout --signal=TERM --kill-after=120 "${RUN_TIMEOUT_SECONDS}" \
        "${OPENCODE_BIN}" run \
        --auto \
        --model "${OPENCODE_MODEL}" \
        --dir "${REPO_ROOT}" \
        "${PROMPT}" \
        >>"${LOG_FILE}" 2>&1
else
    timeout --signal=TERM --kill-after=120 "${RUN_TIMEOUT_SECONDS}" \
        "${CLAUDE_BIN}" -p "${PROMPT}" \
        --dangerously-skip-permissions \
        --verbose \
        >>"${LOG_FILE}" 2>&1
fi
EXIT_CODE=$?
set -e

if [[ ${EXIT_CODE} -eq 0 ]]; then
    log "=== 完成，exit=0 ==="
    write_status "success" "ok"
elif [[ ${EXIT_CODE} -eq 124 || ${EXIT_CODE} -eq 137 ]]; then
    log "=== 超时终止（${RUN_TIMEOUT_SECONDS}s），exit=${EXIT_CODE} ==="
    write_status "timeout" "exceeded ${RUN_TIMEOUT_SECONDS}s"
else
    log "=== 失败，exit=${EXIT_CODE} ==="
    write_status "failed" "${RUNTIME} exit ${EXIT_CODE}"
fi

exit "${EXIT_CODE}"

# ---------------------------------------------------------------------------
# crontab 安装（周一至周五 21:03，避开整点高峰）：
#
#   crontab -e
#   3 21 * * 1-5 /home/zhangbeiqing/programer/AI-Value-Investing-Agent/scripts/cron_daily_data_prep.sh
#
# 查看结果：
#   cat logs/cron_daily_prep/latest_status.json
#   tail -f logs/cron_daily_prep/$(date +%F).log
#
# opencode 自己的运行日志（排查 agent 内部问题时看）：
#   ~/.local/share/opencode/log/
#
# 注意：WSL 关闭（wsl --shutdown 或 Windows 关机）时 cron 不会触发，也不会补跑。
# 需要跨重启也能补跑，就把触发点改到 Windows 任务计划调 wsl.exe。
# ---------------------------------------------------------------------------
