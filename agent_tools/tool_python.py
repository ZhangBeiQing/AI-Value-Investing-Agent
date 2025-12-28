#!/usr/bin/env python
"""Python Interpreter MCP Tool
================================
本模块提供一个直接运行在 AI-TRADER 项目本地环境中的 Python 解释器工具。
为了方便后续维护，这里用注释写下设计思路、规则与规范：

设计思路
---------
1. 延续 deepResearch/tool_python 的交互体验，但移除依赖 sandbox_fusion，改为本地子进程执行。
2. 通过 FastMCP 将解释器暴露为 MCP 工具，便于 LangChain Agent 复用统一的工具/提示模板。
3. 为了易调试，执行逻辑倾向“透明”——保存原始代码、标准输出/错误以及超时信息。

设计规则
---------
1. 解释器只接受字符串形式的代码，不做额外上下文注入；所有依赖需在代码中显式声明。
2. 任何输出都必须使用 ``print``（提示在工具描述和 LangChain prompt 中保持一致）。
3. 执行时间受 ``PYTHON_TOOL_TIMEOUT`` 控制（默认 30s），防止阻塞主代理。
4. 为避免日志爆炸，stdout/stderr 统一截断在 ``PYTHON_TOOL_MAX_OUTPUT``（默认 20000 字符）。
5. 以临时文件 + ``python -u`` 子进程方式运行，确保和生产环境的解释器一致，且方便调试。

设计规范
---------
- 代码解析顺序：优先解析 <code>...</code>，其次 Markdown ``` ```，最后直接把输入视为代码。
- 文件结构统一 FastMCP 风格：加载 .env、定义工具、最后提供命令行入口。
- 关键函数都提供 docstring，便于二次开发或迁移到其他 Agent 框架。
- 在工具描述中明确“本地执行/无沙盒”的风险提示，方便在 prompt 管控层做安全告知。

解释器作用
-----------
- 作为 LangChain Agent 的通用 Python scratchpad，可用于数据清洗、快速实验、验证数学/统计结论。
- **替代原 Math MCP**：无论是 `add`, `multiply` 还是更复杂的计算，都可以直接编写 Python 代码完成。
- 在分析股票数据时，可以和项目中的数据目录直接交互（默认在仓库根目录运行）。
- 通过 MCP 协议统一管理，方便和其它工具（行情、搜索、交易模拟）一同注册到同一 Agent 会话。
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastmcp import FastMCP

from agent_tools.logging_utils import init_tool_logger

load_dotenv()

# ---------------------------------------------------------------------------
# 可调参数：可通过 .env 覆盖，便于在不同机器/CI 环境中控制执行行为
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT = int(os.getenv("PYTHON_TOOL_TIMEOUT", "30"))
MAX_OUTPUT_LENGTH = int(os.getenv("PYTHON_TOOL_MAX_OUTPUT", "20000"))
EXECUTION_ROOT = Path(os.getenv("PYTHON_TOOL_WORKDIR", Path.cwd()))
EXECUTION_ROOT.mkdir(parents=True, exist_ok=True)
RAW_OUTPUT_DIR = Path(os.getenv("PYTHON_TOOL_OUTPUT_DIR", Path("logs") / "python_tool" / "raw_outputs"))
RAW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

mcp = FastMCP("PythonInterpreter")
logger = init_tool_logger(mcp.name)


@dataclass
class ExecutionResult:
    """结构化保存一次执行的所有关键信息。"""

    stdout: str
    stderr: str
    exit_code: int
    duration: float
    timed_out: bool = False
    truncated: bool = False
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None


CODE_XML_PATTERN = re.compile(r"<code>(.*?)</code>", re.DOTALL | re.IGNORECASE)
CODE_FENCE_PATTERN = re.compile(r"```(?:python)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def extract_user_code(raw: str) -> str:
    """从用户输入中提取代码，支持 XML 与 Markdown 包裹。"""

    if raw is None:
        return ""

    match = CODE_XML_PATTERN.search(raw)
    if match:
        return match.group(1).strip()

    match = CODE_FENCE_PATTERN.search(raw)
    if match:
        return match.group(1).strip()

    return raw.strip()


def clamp_output(text: str) -> tuple[str, bool]:
    """限制输出长度，返回 (文本, 是否被截断)。"""

    if len(text) <= MAX_OUTPUT_LENGTH:
        return text, False
    truncated_text = text[:MAX_OUTPUT_LENGTH] + "\n...<truncated>"
    return truncated_text, True


def persist_truncated_output(label: str, content: str) -> str:
    """将被截断的原始输出写入文件，返回相对路径字符串。"""

    timestamp = int(time.time() * 1000)
    filename = f"{label}_{timestamp}.txt"
    file_path = RAW_OUTPUT_DIR / filename
    file_path.write_text(content, encoding="utf-8")
    return str(file_path.resolve())


def run_python_code(code: str, timeout: Optional[int] = None) -> ExecutionResult:
    """在本地子进程中执行代码，并返回结构化结果。"""

    if not code.strip():
        return ExecutionResult(
            stdout="",
            stderr="[PythonInterpreter] Received empty code block.",
            exit_code=1,
            duration=0,
        )

    timeout = timeout or DEFAULT_TIMEOUT
    normalized = textwrap.dedent(code)
    logger.info("python.run start: timeout=%s, length=%d", timeout, len(normalized))

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp_file:
        tmp_file.write(normalized)
        tmp_path = Path(tmp_file.name)

    start = time.perf_counter()
    try:
        completed = subprocess.run(
            [sys.executable, "-u", str(tmp_path)],
            cwd=str(EXECUTION_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        duration = time.perf_counter() - start
        stdout_raw = completed.stdout or ""
        stderr_raw = completed.stderr or ""
        stdout, stdout_trunc = clamp_output(stdout_raw)
        stderr, stderr_trunc = clamp_output(stderr_raw)
        stdout_path = persist_truncated_output("stdout", stdout_raw) if stdout_trunc else None
        stderr_path = persist_truncated_output("stderr", stderr_raw) if stderr_trunc else None
        result = ExecutionResult(
            stdout=stdout,
            stderr=stderr,
            exit_code=completed.returncode,
            duration=duration,
            timed_out=False,
            truncated=stdout_trunc or stderr_trunc,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
    except subprocess.TimeoutExpired as exc:
        duration = time.perf_counter() - start
        stdout = exc.stdout or ""
        stderr = (exc.stderr or "") + "\n[PythonInterpreter] Execution timed out."
        stdout_str, stdout_trunc = clamp_output(stdout)
        stderr_str, stderr_trunc = clamp_output(stderr)
        stdout_path = persist_truncated_output("stdout_timeout", stdout) if stdout_trunc else None
        stderr_path = persist_truncated_output("stderr_timeout", stderr) if stderr_trunc else None
        result = ExecutionResult(
            stdout=stdout_str,
            stderr=stderr_str,
            exit_code=-1,
            duration=duration,
            timed_out=True,
            truncated=stdout_trunc or stderr_trunc,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
    except Exception:
        logger.exception("python.run 执行期间发生未捕获异常")
        raise
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

    logger.info(
        "python.run done: exit=%s, duration=%.2fs, timed_out=%s, truncated=%s",
        result.exit_code,
        result.duration,
        result.timed_out,
        result.truncated,
    )
    return result


def format_execution_result(result: ExecutionResult) -> str:
    """将 ExecutionResult 转为 Agent 友好的文本。"""

    sections = []
    meta = [
        f"exit_code: {result.exit_code}",
        f"duration: {result.duration:.2f}s",
        f"timed_out: {result.timed_out}",
        f"output_truncated: {result.truncated}",
    ]
    sections.append("[python-meta] " + ", ".join(meta))

    if result.stdout:
        sections.append("[stdout]\n" + result.stdout)
        if result.stdout_path:
            sections.append(f"[stdout-file] Full output saved at: {result.stdout_path}")
    if result.stderr:
        sections.append("[stderr]\n" + result.stderr)
        if result.stderr_path:
            sections.append(f"[stderr-file] Full output saved at: {result.stderr_path}")

    if len(sections) == 1:
        sections.append("[info] Finished execution without output. Use print() to emit values.")

    return "\n\n".join(sections)


@mcp.tool()
def run_python(code: str, timeout_seconds: Optional[int] = None) -> str:
    """Execute arbitrary Python code on the local interpreter.
    
    This tool allows AI agents to execute Python code directly in the AI-TRADER project environment.
    It supports multiple code input formats and provides detailed execution feedback.

    Args:
        code: Python source code. Supports multiple formats:
            - Raw string: "print('hello')"
            - Markdown code block: ```python\nprint('hello')\n```
            - XML code block: <code>print('hello')</code>
        timeout_seconds: Execution timeout in seconds, defaults to PYTHON_TOOL_TIMEOUT (30s).

    Returns:
        Structured execution result with stdout, stderr, exit code, and timing information.

    Important Notes:
        - All output must be explicitly printed using print() statements
        - Code executes in the project root directory with access to all project files
        - Maximum output length is 4000 characters (truncated if exceeded)
        - Default timeout is 30 seconds (configurable via PYTHON_TOOL_TIMEOUT)
        - No sandboxing - code runs with same permissions as the main process
    """

    logger.info(
        "run_python 请求: timeout=%s, raw_length=%d",
        timeout_seconds,
        len(code or ""),
    )
    user_code = extract_user_code(code)
    result = run_python_code(user_code, timeout_seconds)
    formatted = format_execution_result(result)
    logger.info(
        "run_python 完成: exit=%s, duration=%.2fs",
        result.exit_code,
        result.duration,
    )
    return formatted


if __name__ == "__main__":
    port = int(os.getenv("PYTHON_HTTP_PORT", "8005"))
    mcp.run(transport="streamable-http", port=port)
