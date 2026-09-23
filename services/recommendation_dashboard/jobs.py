"""Durable, isolated single-stock research jobs for the local dashboard."""

from __future__ import annotations

import csv
import fcntl
import json
import os
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

from core.logging import get_logger
from services.recommendation_dashboard.research import SYMBOL_RE
from services.selection_system.master_universe import load_master_universe, save_master_universe
from services.selection_system.models import MasterUniverseDocument, MasterUniverseStock
from services.selection_system.paths import SelectionSystemPaths
from shared_data_access.stock_name_lookup import lookup_cached_stock_names
from shared_data_access.market_calendar import inspect_market_session
from commons.stock_utils import normalize_symbol, parse_symbol


LOGGER = get_logger("DashboardResearchJobs")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNNING = {"queued", "preparing_data", "financial_research", "building_research", "debating", "validating", "publishing", "cancelling"}
TERMINAL = {"complete", "failed", "cancelled"}
CANCELLABLE = RUNNING - {"publishing", "cancelling"}
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
BLOCKING_FINANCIAL_SKIP_REASONS = {
    "latest_report_pdf_missing",
    "latest_report_markdown_conversion_failed",
    # Backward-compatible guard for preparation output produced by older code.
    "latest_report_markdown_missing_conversion_disabled",
}


def _job_dir(data_dir: Path) -> Path:
    return data_dir / "web_research_runs" / "jobs"


def _job_path(data_dir: Path, job_id: str) -> Path:
    if not uuid.UUID(job_id).hex == job_id.replace("-", ""):
        raise ValueError("任务编号无效")
    return _job_dir(data_dir) / f"{job_id}.json"


def _write_job(data_dir: Path, job: dict[str, Any]) -> None:
    path = _job_path(data_dir, job["id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    job["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=".job-", delete=False) as handle:
        temp_path = Path(handle.name)
        json.dump(job, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    temp_path.replace(path)


def _job_log_tail(data_dir: Path, job_id: str, limit: int = 64000) -> str:
    log_path = _job_path(data_dir, job_id).with_suffix(".log")
    if not log_path.is_file():
        return ""
    with log_path.open("rb") as stream:
        stream.seek(0, 2)
        stream.seek(max(0, stream.tell() - limit))
        return stream.read().decode("utf-8", errors="replace")


def _friendly_job_error(data_dir: Path, job: dict[str, Any], exc: Exception) -> str:
    """Extract the actionable provider error instead of exposing a huge command."""
    log_tail = ANSI_ESCAPE_RE.sub("", _job_log_tail(data_dir, job["id"]))
    normalized = log_tail.lower()
    if "insufficient balance" in normalized:
        return "OpenCode 模型账户余额不足（Insufficient Balance），请充值或更换模型后重试"
    if "rate limit" in normalized or "too many requests" in normalized:
        return "OpenCode 模型服务触发限流，请稍后重试"
    if "unauthorized" in normalized or "invalid api key" in normalized:
        return "OpenCode 模型认证失败，请检查 API Key"
    if isinstance(exc, subprocess.TimeoutExpired):
        return "OpenCode 分析超时，请查看任务日志后重试"
    if isinstance(exc, subprocess.CalledProcessError):
        if "data refresh failed" in normalized or "shared data 刷新失败" in log_tail:
            matches = re.findall(r"^-\s+([^\n]+)$", log_tail, flags=re.MULTILINE)
            detail = matches[-1].strip() if matches else "基础数据缓存未准备完成"
            return f"单股数据准备失败：{detail}；请查看任务日志"
        return f"OpenCode 分析进程异常退出（状态码 {exc.returncode}），请查看任务日志"
    return f"{type(exc).__name__}: {exc}"


def _financial_preparation_error(payload: dict[str, Any], symbol: str) -> str | None:
    """Return a blocking error when a known latest report lacks usable source text."""
    for item in payload.get("skipped_items") or []:
        if not isinstance(item, dict) or item.get("symbol") != symbol:
            continue
        reason = item.get("skip_reason")
        if reason not in BLOCKING_FINANCIAL_SKIP_REASONS:
            continue
        report_date = item.get("latest_report_date") or "未知日期"
        announcement_id = item.get("latest_announcement_id") or "未知编号"
        if reason == "latest_report_pdf_missing":
            detail = "财报 PDF 下载失败或尚未缓存"
        elif reason == "latest_report_markdown_conversion_failed":
            detail = "财报 PDF 转 Markdown 失败"
        else:
            detail = "财报 Markdown 未准备完成"
        return (
            f"最新正式财报不可用：{report_date}（公告 {announcement_id}），{detail}；"
            "为避免生成缺少财报依据的裁决，任务已停止"
        )
    return None


def read_job(data_dir: Path, job_id: str) -> dict[str, Any] | None:
    path = _job_path(data_dir, job_id)
    if not path.is_file():
        return None
    job = json.loads(path.read_text(encoding="utf-8"))
    if not job.get("stock_name"):
        job["stock_name"] = parse_symbol(job["symbol"]).stock_name or job["symbol"]
    if job.get("status") == "failed" and "CalledProcessError:" in str(job.get("message", "")):
        friendly = _friendly_job_error(data_dir, job, subprocess.CalledProcessError(1, "opencode"))
        if friendly != job["message"]:
            job["message"] = friendly
            _write_job(data_dir, job)
    if job["status"] in RUNNING and job.get("pid"):
        try:
            os.kill(job["pid"], 0)
        except ProcessLookupError:
            job["status"] = "failed"
            job["message"] = "后台进程已退出；请查看日志后重试"
            _write_job(data_dir, job)
    if job["status"] == "debating":
        from services.trading.debate_pipeline import debate_symbol_dir

        workspace = data_dir / "web_research_runs" / job_id / "skill_runs" / job["date"] / "fixed_tracked"
        debate_dir = debate_symbol_dir(workspace, job["symbol"])
        expected = [
            "advocates/bull/opening.json", "advocates/bear/opening.json",
            "advocates/bull/rebuttal.json", "advocates/bear/rebuttal.json",
            "jury/juror_01/ballot.json", "jury/juror_02/ballot.json", "jury/juror_03/ballot.json",
            "final/vote_summary.json", "final/stock_verdict.json",
        ]
        finished = sum((debate_dir / name).is_file() for name in expected)
        job["message"] = f"AI 辩论进行中 · {finished}/{len(expected)} 份阶段文件已生成"
    log_tail = _job_log_tail(data_dir, job_id, limit=24000)
    if log_tail:
        job["log_tail"] = log_tail
    return job


def latest_jobs(data_dir: Path) -> list[dict[str, Any]]:
    jobs = []
    if not _job_dir(data_dir).is_dir():
        return jobs
    for path in _job_dir(data_dir).glob("*.json"):
        try:
            job = read_job(data_dir, path.stem)
            if job:
                job.pop("log_tail", None)
                jobs.append(job)
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            LOGGER.warning("跳过损坏的网页研究任务: %s", path)
    return sorted(jobs, key=lambda item: item["created_at"], reverse=True)[:100]


def job_result(data_dir: Path, job_id: str) -> dict[str, Any] | None:
    job = read_job(data_dir, job_id)
    if job is None:
        return None
    from services.trading.debate_pipeline import debate_symbol_dir

    workspace = data_dir / "web_research_runs" / job_id / "skill_runs" / job["date"] / "fixed_tracked"
    debate_dir = debate_symbol_dir(workspace, job["symbol"])
    verdict_path = debate_dir / "final" / "stock_verdict.json"
    stage_files = {
        "bull": "advocates/bull/opening.json", "bear": "advocates/bear/opening.json",
        "bull_rebuttal": "advocates/bull/rebuttal.json", "bear_rebuttal": "advocates/bear/rebuttal.json",
        "juror_01": "jury/juror_01/ballot.json", "juror_02": "jury/juror_02/ballot.json",
        "juror_03": "jury/juror_03/ballot.json", "vote_summary": "final/vote_summary.json",
    }
    stages = {}
    for key, relative_path in stage_files.items():
        path = debate_dir / relative_path
        if path.is_file():
            try:
                stages[key] = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                LOGGER.warning("独立研究阶段文件不是有效 JSON: %s", path)
    research_dir = workspace / "04_stock_research"
    research = next(research_dir.glob(f"*_{job['symbol']}_{job['date']}_research.md"), None) if research_dir.is_dir() else None
    verdict = None
    if verdict_path.is_file():
        try:
            verdict = json.loads(verdict_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            LOGGER.warning("独立研究最终裁决尚未写完: %s", verdict_path)
    return {
        "job": job,
        "verdict": verdict,
        "stages": stages,
        "research": research.read_text(encoding="utf-8") if research else None,
    }


def _analysis_date(data_dir: Path) -> str:
    today = date.today().isoformat()
    session = inspect_market_session(today, market="CN", base_dir=data_dir)
    candidates = [today, session.previous_trading_day] if session.is_trading_day else [session.previous_trading_day]
    for candidate in candidates:
        official_dir = data_dir / "skill_runs" / candidate / "fixed_tracked"
        if all((official_dir / name).is_file() for name in ("01_global_context.md", "03_stock_analysis_input.md")):
            return candidate
    return candidates[0]


def start_job(data_dir: Path, symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    if not SYMBOL_RE.fullmatch(symbol):
        raise ValueError("股票代码格式不正确")
    date_text = _analysis_date(data_dir)
    official_dir = data_dir / "skill_runs" / date_text / "fixed_tracked"
    if not all((official_dir / name).is_file() for name in ("01_global_context.md", "03_stock_analysis_input.md")):
        raise ValueError(f"{date_text} 的每日公共研究输入尚未就绪，请等每日数据准备完成后再分析")
    _job_dir(data_dir).mkdir(parents=True, exist_ok=True)
    lock_path = _job_dir(data_dir) / ".start.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for existing in latest_jobs(data_dir):
            if existing["symbol"] == symbol and existing["date"] == date_text and existing["status"] in RUNNING:
                return existing
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id, "symbol": symbol, "stock_name": parse_symbol(symbol).stock_name or symbol,
            "date": date_text, "status": "queued",
            "message": "等待后台任务启动", "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "updated_at": "", "pid": None,
        }
        _write_job(data_dir, job)
        log_path = _job_path(data_dir, job_id).with_suffix(".log")
        command = [sys.executable, str(PROJECT_ROOT / "scripts" / "run_dashboard_research_job.py"), job_id, "--data-dir", str(data_dir)]
        try:
            with log_path.open("ab") as log:
                process = subprocess.Popen(command, cwd=PROJECT_ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        except OSError as exc:
            _set_stage(data_dir, job, "failed", f"无法启动后台进程：{exc}")
            raise
        current = json.loads(_job_path(data_dir, job_id).read_text(encoding="utf-8"))
        current["pid"] = process.pid
        _write_job(data_dir, current)
        return current


def _worker_matches_job(pid: int, job_id: str) -> bool:
    """Avoid signaling a recycled PID that no longer belongs to this job."""
    try:
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        return False
    return "run_dashboard_research_job.py" in command and job_id in command


def cancel_job(data_dir: Path, job_id: str) -> dict[str, Any]:
    """Cancel one detached worker and its child processes before publication."""
    job = read_job(data_dir, job_id)
    if job is None:
        raise ValueError("任务不存在")
    if job["status"] in TERMINAL:
        return job
    if job["status"] == "publishing":
        raise ValueError("任务正在写入正式决策，不能取消，请等待发布完成")
    if job["status"] == "cancelling":
        return job
    if job["status"] not in CANCELLABLE:
        raise ValueError(f"当前任务状态不能取消：{job['status']}")
    pid = job.get("pid")
    if not isinstance(pid, int) or pid <= 1:
        raise ValueError("任务进程编号无效，无法安全取消")
    if not _worker_matches_job(pid, job_id) or os.getpgid(pid) != pid:
        raise ValueError("后台进程身份校验失败，拒绝终止可能无关的进程")
    job["status"] = "cancelling"
    job["message"] = "正在取消后台研究任务"
    _write_job(data_dir, job)
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    job["status"] = "cancelled"
    job["message"] = "任务已由用户取消；隔离目录中的中间产物未发布、未记账"
    _write_job(data_dir, job)
    LOGGER.info("网页单股研究已取消: %s pid=%s symbol=%s", job_id, pid, job["symbol"])
    return job


def resolve_stock_name(data_dir: Path, name: str) -> list[dict[str, str]]:
    name = name.strip()
    if not name or len(name) > 80:
        raise ValueError("请输入有效的股票名称")
    universe = load_master_universe(SelectionSystemPaths.from_base_dir(data_dir))
    matches = {stock.symbol: {"symbol": stock.symbol, "name": stock.name, "in_universe": True}
               for stock in universe.stocks if stock.name == name or stock.symbol == name.upper()}
    mapping_path = data_dir / "global_cache" / "symbol_stock_name_mapping.csv"
    if mapping_path.is_file():
        with mapping_path.open("r", encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                symbol = (row.get("symbol") or "").upper().strip()
                if (row.get("stock_name") or "").strip() == name and SYMBOL_RE.fullmatch(symbol):
                    matches.setdefault(symbol, {"symbol": symbol, "name": name, "in_universe": False})
    for item in lookup_cached_stock_names(name, base_dir=data_dir):
        matches.setdefault(item["symbol"], {**item, "in_universe": False})
    return list(matches.values())


def add_stock_to_universe(data_dir: Path, symbol: str, name: str) -> dict[str, str]:
    candidates = resolve_stock_name(data_dir, name)
    selected = next((item for item in candidates if item["symbol"] == symbol), None)
    if selected is None:
        raise ValueError("股票名称与代码未能在本地映射中相互核实")
    if selected["in_universe"]:
        return selected
    paths = SelectionSystemPaths.from_base_dir(data_dir)
    lock_path = paths.universe_dir / ".web-add.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        document = load_master_universe(paths)
        existing = next((stock for stock in document.stocks if stock.symbol == symbol), None)
        if existing:
            if existing.name != name:
                raise ValueError(f"代码 {symbol} 已以名称 {existing.name} 存在于股票宇宙")
            return {"symbol": symbol, "name": name, "in_universe": True}
        updated = MasterUniverseDocument(
            stocks=[*document.stocks, MasterUniverseStock(symbol=symbol, name=name)],
            schema_version=document.schema_version,
            universe_name=document.universe_name,
            description=document.description,
        )
        save_master_universe(updated, paths)
    return {"symbol": symbol, "name": name, "in_universe": True}


def _set_stage(data_dir: Path, job: dict[str, Any], stage: str, message: str) -> None:
    job["status"] = stage
    job["message"] = message
    _write_job(data_dir, job)
    LOGGER.info("网页单股研究: %s %s %s", job["symbol"], stage, message)


def run_job(data_dir: Path, job_id: str) -> None:
    """Run data preparation and an agent debate in an isolated workspace."""
    job = read_job(data_dir, job_id)
    if job is None:
        raise ValueError("任务不存在")
    symbol, run_date = job["symbol"], job["date"]
    official_dir = data_dir / "skill_runs" / run_date / "fixed_tracked"
    if not all((official_dir / name).is_file() for name in ("01_global_context.md", "03_stock_analysis_input.md")):
        _set_stage(data_dir, job, "failed", "当天公共研究输入尚未就绪，请先完成每日数据准备")
        return
    workspace = data_dir / "web_research_runs" / job_id / "skill_runs" / run_date / "fixed_tracked"
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        opencode = shutil.which("opencode")
        if not opencode:
            raise RuntimeError("没有找到 opencode CLI，无法运行 Agent 深研与辩论")
        model = os.environ.get("DASHBOARD_RESEARCH_MODEL", "deepseek/deepseek-v4-flash")
        _set_stage(data_dir, job, "preparing_data", "准备该股票的行情、财报与公告缓存")
        subprocess.run(
            [sys.executable, "scripts/manage_daily_data.py", "--date", run_date, "--symbols", symbol],
            cwd=PROJECT_ROOT, check=True,
        )
        from services.recommendation_dashboard.data import _price_points
        from commons.stock_utils import parse_symbol

        closes, price_date, _ = _price_points(data_dir, symbol, parse_symbol(symbol).stock_name)
        if run_date not in closes:
            raise RuntimeError(f"{symbol} 缺少分析日 {run_date} 的准确收盘价（缓存最新为 {price_date or '无缓存'}）；拒绝用其他日期代替")
        financial_prep = workspace / "financial_prepare.json"
        subprocess.run(
            [sys.executable, "scripts/prepare_financial_report_skill.py", "--date", run_date,
             "--symbols", symbol, "--sync-first", "--output", str(financial_prep)],
            cwd=PROJECT_ROOT, check=True,
        )
        financial_payload = json.loads(financial_prep.read_text(encoding="utf-8"))
        preparation_error = _financial_preparation_error(financial_payload, symbol)
        if preparation_error:
            raise RuntimeError(preparation_error)
        ready_items = financial_payload.get("ready_items") or []
        if ready_items:
            _set_stage(data_dir, job, "financial_research", "Agent 正在完成该股票的最新财报深研")
            financial_prompt = (
                f"用户在本地网页明确授权对 {symbol} 执行单股财报深研，分析日期 {run_date}。"
                "完整读取并严格执行 .codex/skills/financial-report-summary/SKILL.md 和其角色模板。"
                f"只处理 {financial_prep} 中该股票的 ready_items，不扩展其他股票。"
                "按 skill 的角色分工完成报告、质量门禁和 summary_index 注册。"
                "不得修改任何交易决策、仓位或账本。完成后报告输出文件路径。"
            )
            subprocess.run([opencode, "run", "--auto", "--model", model, "--dir", str(PROJECT_ROOT), financial_prompt],
                           cwd=PROJECT_ROOT, check=True, timeout=9000)
            for item in ready_items:
                if not Path(item["output_path"]).is_file():
                    raise RuntimeError(f"财报深研未产出：{item['output_path']}")
        _set_stage(data_dir, job, "building_research", "生成单股快照和研究包")
        from services.pipeline.daily_pipeline import SKILL_FLOW_CONFIG, run_book_pipeline

        run_book_pipeline(
            run_date, output_dir=workspace, symbols=[symbol], prompt_config=SKILL_FLOW_CONFIG,
            signature="book-fixed_tracked", book_type="fixed_tracked", source_data_root=data_dir,
            research_cache_root=data_dir / "web_research_runs" / job_id / "artifact_cache",
            allow_news_refresh=False,
        )
        # The already-validated daily common context/policy is the source of truth.
        for name in ("01_global_context.md", "03_stock_analysis_input.md"):
            shutil.copy2(official_dir / name, workspace / name)
        if not list((workspace / "04_stock_research").glob(f"*_{symbol}_{run_date}_research.md")):
            raise RuntimeError("单股研究包未生成")
        _set_stage(data_dir, job, "debating", "Agent 正在进行正反辩论、反驳和三名裁判裁决")
        from services.trading.debate_pipeline import debate_symbol_dir, prepare_debate_directories

        prepare_debate_directories(workspace, symbol)
        prompt = (
            f"用户已在本地网页明确指定 {symbol} 为 {run_date} 的唯一 P0 股票，授权执行单股研究辩论。"
            f"先完整读取 .codex/skills/auto-trading-fixed-tracked/SKILL.md 及各角色 references。"
            f"本任务的隔离目录是 {workspace}；其中的 01、02、03、04 是本次唯一输入。"
            f"调用 scripts/manage_debate.py 时必须传 --base-dir {workspace.parent.parent} --date {run_date} "
            f"--book-type fixed_tracked --symbol {symbol}，绝不能使用默认正式目录。"
            "仅执行该 skill 的阶段 B：Bull/Bear opening、复用会话 rebuttal、三名独立 Juror、"
            "程序化 aggregate、唯一 finalizer、validate。角色只写各自文件，主 Agent 不代写。"
            f"聚合时从人工持仓文件核实 {symbol} 实际持股数，不得猜测。"
            "不得修改正式 data/skill_runs、股票宇宙、05_decision.json、任何账本或真实持仓；"
            "不执行交易后处理。最终只报告文件路径和动作。"
        )
        subprocess.run([opencode, "run", "--auto", "--model", model, "--dir", str(PROJECT_ROOT), prompt],
                       cwd=PROJECT_ROOT, check=True, timeout=9000)
        _set_stage(data_dir, job, "validating", "检查全部辩论文件和最终裁决")
        from services.trading.debate_pipeline import validate_debate_artifacts

        errors = validate_debate_artifacts(workspace, symbol, require_verdict=True)
        if errors:
            raise RuntimeError("辩论产物不完整：" + "; ".join(errors[:4]))
        _set_stage(data_dir, job, "publishing", "合入正式决策汇总与 Agent 虚拟账本")
        from services.trading.dashboard_decision_publisher import publish_dashboard_verdict

        verdict_path = debate_symbol_dir(workspace, symbol) / "final" / "stock_verdict.json"
        verdict = json.loads(verdict_path.read_text(encoding="utf-8"))
        publish_dashboard_verdict(data_dir, workspace, symbol=symbol, run_date=run_date, verdict=verdict)
        _set_stage(data_dir, job, "complete", "分析已完成并写入正式决策与 Agent 虚拟账本；真实持仓未修改")
    except Exception as exc:
        LOGGER.exception("网页单股研究失败: %s", job_id)
        _set_stage(data_dir, job, "failed", _friendly_job_error(data_dir, job, exc))
