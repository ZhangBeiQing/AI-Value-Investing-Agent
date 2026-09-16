#!/usr/bin/env bash
# 每晚定时触发 daily-data-preparation skill 的 cron 包装脚本。
#
# 默认用 opencode + deepseek-v4-flash 跑。
# 需要临时换回 claude 时：RUNTIME=claude scripts/cron_daily_data_prep.sh
# 需要换模型时：      OPENCODE_MODEL=openai/gpt-5.6 scripts/cron_daily_data_prep.sh
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
# ⚠️ 手动补跑时**必须 detach**，否则父会话（终端、Claude Code 的后台 bash、SSH）一退出，
#    整棵进程树连带 opencode 一起被杀，跑到一半的活全丢：
#      setsid nohup scripts/cron_daily_data_prep.sh 2026-08-21 >/dev/null 2>&1 &
#    2026-08-22 就踩过这个坑：Step 1-3 都跑完了，02:01 会话结束把 opencode 带走，Step 4/5 没了。
#    cron 自己拉起时不受影响（cron 的子进程本来就不挂在任何交互会话上）。
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
# 不钉的话默认模型由 opencode 自己决定，跨版本会漂。
#
# 为什么默认不是 gpt-5.6：它在无头 `opencode run` 下会间歇性把首条回复发到 final 频道，
# turn 立刻结束、后续工具调用全被丢弃，十几秒 exit 0 且零产物。实测 4 轮中了 2 轮
# （2026-08-21、2026-08-24），50% 失败率，不适合放在无人值守主路径上。
# deepseek-v4-flash 在 2026-08-17 跑出过完整 01-04，且便宜约两个数量级。
# 想手动用 gpt-5.6：OPENCODE_MODEL=openai/gpt-5.6 （注意别写成 openai/gpt-5.6-sol，
# 那是 config 里另一个独立条目，额度账不一样）。
OPENCODE_MODEL="${OPENCODE_MODEL:-deepseek/deepseek-v4-flash}"

# cron 只给一个极简 PATH，这里显式补齐 node（opencode/claude 都依赖）与常规系统路径。
export PATH="${NODE_BIN}:${VENV}/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export HOME="${HOME:-/home/zhangbeiqing}"

# 单次运行的整体上限，防止卡死的任务一直挂着占资源（2.5 小时）。
RUN_TIMEOUT_SECONDS="${RUN_TIMEOUT_SECONDS:-9000}"

cd "${REPO_ROOT}"

# 允许外部钉住起算时间，供 VERIFY_ONLY 回放历史日期时绕过新鲜度检查。
START_EPOCH="${START_EPOCH_OVERRIDE:-$(date +%s)}"

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
    # status 取值：running / success / skipped / timeout / failed
    # "running" 在拉起 agent 之前就写，否则整轮 50-90 分钟里这个文件一直显示上一轮的
    # 结果，看的人会误判成"cron 今天没触发"。
    cat >"${STATUS_FILE}" <<EOF
{
  "run_date": "${RUN_DATE}",
  "runtime": "${RUNTIME}",
  "model": "${ACTIVE_MODEL:-${OPENCODE_MODEL}}",
  "status": "$1",
  "message": "$2",
  "elapsed_seconds": $(( $(date +%s) - START_EPOCH )),
  "updated_at": "$(date '+%Y-%m-%d %H:%M:%S')",
  "log_file": "${LOG_FILE}"
}
EOF
}

# 产物验收：退出码 0 **不等于**真的跑完。
#
# 2026-08-21 的真实教训：gpt-5.6 把首条状态更新错发到 final 频道，turn 被提前结束，
# 之后所有工具调用被静默丢弃，38 秒就 exit 0，wrapper 照实写了 success，
# 于是 latest_status.json 骗了人一整天。
#
# 所以成功判定以"产物真的落盘"为准，退出码只降级为必要条件。
# 除了存在性，还必须检查 Step 5 产物的 mtime 晚于本轮启动——否则幂等重跑时，
# 上一轮留下的旧文件会让一个什么都没干的空转轮顺利通过验收。
verify_artifacts() {
    local d="${RUN_DATE}"
    local compact="${d//-/}"
    local missing=()
    local f

    # Step 1 门禁 + Step 3 两路研究产出 + Step 5 的 01-03
    for f in \
        "data/global_cache/macro_objective_panel/daily_snapshots/${d}.json" \
        "data/selection_runs/${d}/12_quant_prefilter_short.csv" \
        "data/macro_economy/${compact}.md" \
        "data/selection_runs/${d}/06_hot_news_state.json" \
        "data/skill_runs/${d}/run_manifest.json" \
        "data/skill_runs/${d}/fixed_tracked/01_global_context.md" \
        "data/skill_runs/${d}/fixed_tracked/02_basic_snapshot_payload.json" \
        "data/skill_runs/${d}/fixed_tracked/03_agent_input.md" \
        "data/skill_runs/${d}/fixed_tracked/03_stock_analysis_input.md" ; do
        [[ -s "${REPO_ROOT}/${f}" ]] || missing+=("${f} 缺失或为空")
    done

    # 04_stock_research/ 必须有内容（并发超限的静默空返回就长成空目录）
    local research_dir="${REPO_ROOT}/data/skill_runs/${d}/fixed_tracked/04_stock_research"
    local research_count=0
    if [[ -d "${research_dir}" ]]; then
        research_count="$(find "${research_dir}" -mindepth 1 -maxdepth 1 | wc -l)"
    fi
    if (( research_count == 0 )); then
        missing+=("04_stock_research/ 为空或不存在")
    fi

    # 新鲜度：这两个文件由 Step 5 每轮重写，旧 mtime 说明 Step 5 根本没跑
    local mtime
    for f in \
        "data/skill_runs/${d}/run_manifest.json" \
        "data/skill_runs/${d}/fixed_tracked/01_global_context.md" ; do
        if [[ -s "${REPO_ROOT}/${f}" ]]; then
            mtime="$(stat -c %Y "${REPO_ROOT}/${f}")"
            if (( mtime < START_EPOCH )); then
                missing+=("${f} 是本轮之前的旧文件（Step 5 没跑）")
            fi
        fi
    done

    if (( ${#missing[@]} > 0 )); then
        printf '%s\n' "${missing[@]}"
        return 1
    fi
    log "产物验收通过（04_stock_research 逐股目录 ${research_count} 个）"
    return 0
}

# 只跑产物验收、不拉起任何 agent，用于排查某一天到底缺什么：
#   VERIFY_ONLY=1 scripts/cron_daily_data_prep.sh 2026-08-21
# 回放历史日期时新鲜度检查会误报，用 START_EPOCH_OVERRIDE=0 关掉它。
if [[ -n "${VERIFY_ONLY:-}" ]]; then
    LOG_FILE=/dev/stdout
    printf '产物验收 run_date=%s（START_EPOCH=%s）\n' "${RUN_DATE}" "${START_EPOCH}"
    set +e
    MISSING="$(verify_artifacts)"
    VERIFY_OK=$?
    set -e
    if [[ ${VERIFY_OK} -eq 0 ]]; then
        printf '结果：通过\n'
    else
        printf '结果：不通过，缺失如下\n'
        while IFS= read -r item; do
            [[ -n "${item}" ]] && printf '  - %s\n' "${item}"
        done <<<"${MISSING}"
    fi
    exit "${VERIFY_OK}"
fi

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
write_status "running" "agent 已拉起，预计 50-90 分钟"

# 被信号打断时也要落一个终态，否则 latest_status.json 会永远停在 running，
# 看的人分不清"正在跑"和"半夜被 kill 了"。SIGKILL 抓不到，只能覆盖可捕获的那几个。
on_signal() {
    local sig="$1"
    log "=== 收到 SIG${sig}，本轮被终止（会话 teardown / 手动 kill / 系统休眠）==="
    write_status "failed" "killed by SIG${sig} after $(( ($(date +%s) - START_EPOCH) / 60 ))min"
    exit 143
}
trap 'on_signal TERM' TERM
trap 'on_signal INT'  INT
trap 'on_signal HUP'  HUP

# skill 的日期默认就是 today，但这里显式把日期写进 prompt：
# 本轮耗时 50-90 分钟，万一跨过午夜，显式日期能避免中途换天。
PROMPT="开始 ${RUN_DATE} 的数据准备。严格按 daily-data-preparation skill 执行，cur_date=${RUN_DATE}。
这是 cron 无人值守运行，没有人可以实时回答你的问题：
- 遇到需要确认的地方，按 skill 的默认路径继续，把待确认事项写进最终报告，不要停下来等人。
- 不要触发任何交易 skill，不要生成或修改 05_decision.json。
- 财报总结（financial-report-summary）必须由你这个主 agent 亲自执行，不要整体丢给一个 subagent，
  否则它内部的逐股角色派发会失去控制。
- 五个阶段全部跑完后，输出一份包含各阶段状态、实际耗时和验收清单结果的最终报告。"

run_agent() {
    # $1=model。返回被调运行时的退出码。
    local model="$1"
    set +e
    if [[ "${RUNTIME}" == "opencode" ]]; then
        # opencode 的 bash 工具没有后台参数，长命令靠大 timeout 撑（全局 AGENTS.md 已要求 >= 1 小时）。
        # --auto 自动批准权限，等价于 claude 的 --dangerously-skip-permissions。
        timeout --signal=TERM --kill-after=120 "${RUN_TIMEOUT_SECONDS}" \
            "${OPENCODE_BIN}" run \
            --auto \
            --model "${model}" \
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
    local rc=$?
    set -e
    return ${rc}
}

# --- 主循环：秒退且产物为空时换模型自动重试一次 ---
#
# 为什么需要这个：模型可能**把首条回复发到 final 频道**，导致 turn 立刻结束、后续所有
# 工具调用被丢弃，十几秒就 exit 0 而一个产物都没生成（gpt-5.6 实测 4 轮中 2 轮如此：
# 2026-08-21、2026-08-24）。这类失败重试就能过，但同模型重试大概率再中，
# 所以换到**另一个 provider** 的 FALLBACK_MODEL——秒退往往是该家服务侧的问题，
# 换同门模型没有意义。
#
# 只对"秒退"重试。跑了半小时才失败的属于真实链路问题（数据源、PDF 转换、余额），
# 从头重来既慢又贵，而且各阶段本来就幂等，人工按「失败与重跑」补那一段更合适。
FALLBACK_MODEL="${FALLBACK_MODEL:-openai/gpt-5.6}"
FAST_FAIL_SECONDS="${FAST_FAIL_SECONDS:-300}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-2}"

ACTIVE_MODEL="${OPENCODE_MODEL}"
ATTEMPT=1

while : ; do
    ATTEMPT_START="$(date +%s)"
    run_agent "${ACTIVE_MODEL}"
    EXIT_CODE=$?
    ATTEMPT_ELAPSED=$(( $(date +%s) - ATTEMPT_START ))

    # 无论退出码是什么都跑一遍产物验收：超时/失败时也要知道到底缺哪几样，
    # 才能判断是整轮白跑还是只差最后一步（差最后一步的话补跑 Step 5 就行）。
    set +e
    MISSING="$(verify_artifacts)"
    VERIFY_OK=$?
    set -e

    if [[ ${VERIFY_OK} -ne 0 ]]; then
        log "第 ${ATTEMPT} 次尝试（model=${ACTIVE_MODEL}）产物验收未通过，缺失 $(printf '%s' "${MISSING}" | grep -c .) 项："
        while IFS= read -r item; do
            [[ -n "${item}" ]] && log "    - ${item}"
        done <<<"${MISSING}"
    fi

    # 成功，或已经不满足重试条件 → 跳出
    if [[ ${EXIT_CODE} -eq 0 && ${VERIFY_OK} -eq 0 ]]; then
        break
    fi
    if (( ATTEMPT >= MAX_ATTEMPTS )); then
        break
    fi
    if (( ATTEMPT_ELAPSED >= FAST_FAIL_SECONDS )); then
        log "本次尝试跑了 ${ATTEMPT_ELAPSED}s（≥ ${FAST_FAIL_SECONDS}s），属于真实链路失败而非模型秒退，不自动重试。"
        break
    fi
    if [[ "${ACTIVE_MODEL}" == "${FALLBACK_MODEL}" ]]; then
        log "已经在用退路模型 ${FALLBACK_MODEL} 且仍然秒退，不再重试。"
        break
    fi

    log "=== 秒退（${ATTEMPT_ELAPSED}s）且产物为空：判定为模型自杀式结束 turn ==="
    log "    换退路模型重试：${ACTIVE_MODEL} → ${FALLBACK_MODEL}"
    ACTIVE_MODEL="${FALLBACK_MODEL}"
    ATTEMPT=$(( ATTEMPT + 1 ))
    write_status "running" "第 ${ATTEMPT} 次尝试，已换 ${ACTIVE_MODEL}"
done

ELAPSED_MIN=$(( ($(date +%s) - START_EPOCH) / 60 ))

# JSON 单行化：换行换成 ; ，并截断，避免把整张清单塞进状态文件
MISSING_BRIEF="$(printf '%s' "${MISSING}" | tr '\n' ';' | cut -c1-300)"
ATTEMPT_NOTE="attempt=${ATTEMPT} model=${ACTIVE_MODEL}"

if [[ ${EXIT_CODE} -eq 0 && ${VERIFY_OK} -eq 0 ]]; then
    log "=== 完成，exit=0，产物齐全（耗时 ${ELAPSED_MIN} 分钟，${ATTEMPT_NOTE}）==="
    write_status "success" "ok, ${ELAPSED_MIN}min, ${ATTEMPT_NOTE}"
elif [[ ${EXIT_CODE} -eq 0 ]]; then
    # 这就是 2026-08-21 / 2026-08-24 的形态：模型自己把 turn 结束了，什么都没干，还 exit 0
    log "=== 假成功：exit=0 但产物不全（耗时 ${ELAPSED_MIN} 分钟，${ATTEMPT_NOTE}），判定为失败 ==="
    log "    典型原因：模型把首条回复发到 final 频道导致 turn 提前结束，之后工具调用被丢弃。"
    log "    补救：setsid nohup scripts/cron_daily_data_prep.sh ${RUN_DATE} >/dev/null 2>&1 &"
    write_status "failed" "exit 0 but artifacts incomplete after ${ELAPSED_MIN}min, ${ATTEMPT_NOTE}: ${MISSING_BRIEF}"
    EXIT_CODE=2
elif [[ ${EXIT_CODE} -eq 124 || ${EXIT_CODE} -eq 137 ]]; then
    log "=== 超时终止（${RUN_TIMEOUT_SECONDS}s），exit=${EXIT_CODE}（${ATTEMPT_NOTE}）==="
    write_status "timeout" "exceeded ${RUN_TIMEOUT_SECONDS}s, ${ATTEMPT_NOTE}: ${MISSING_BRIEF}"
else
    log "=== 失败，exit=${EXIT_CODE}（耗时 ${ELAPSED_MIN} 分钟，${ATTEMPT_NOTE}）==="
    write_status "failed" "${RUNTIME} exit ${EXIT_CODE}, ${ATTEMPT_NOTE}: ${MISSING_BRIEF}"
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
