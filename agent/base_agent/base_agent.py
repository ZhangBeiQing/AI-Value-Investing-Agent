"""
BaseAgent class - Base class for trading agents
Encapsulates core functionality including MCP tool management, AI agent creation, and trading execution
"""

import os
import json
import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import pandas_market_calendars as mcal  # type: ignore
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI
from langchain.agents import create_agent
from dotenv import load_dotenv
from .deepseek_reasoner_wrapper import DeepseekReasonerWrapper

# Import project tools
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utlity import parse_symbol

from tools.general_tools import (
    extract_conversation,
    extract_reasoning_details,
    extract_tool_messages,
    get_config_value,
    write_config_value,
)
from tools.price_tools import add_no_trade_record
from prompts.agent_prompt import (
    get_agent_system_prompt,
    STOP_SIGNAL,
    extract_json_from_ai_output,
)
from basic_stock_info import basic_info, DEFAULT_PRICE_LOOKBACK_DAYS
from configs.stock_pool import TRACKED_SYMBOLS

from trade_summary import (
    initialize_data_files,
    save_daily_operations,
    process_and_merge_operations,
)

JSON_RESPONSE_FORMAT = {"type": "json_object"}

JSON_RESPONSE_FORMAT = {"type": "json_object"}

# Load environment variables
load_dotenv()


class BaseAgent:
    """
    Base class for trading agents
    
    Main functionalities:
    1. MCP tool management and connection
    2. AI agent creation and configuration
    3. Trading execution and decision loops
    4. Logging and management
    5. Position and configuration management
    """
    
    # Default NASDAQ 100 stock symbols
    DEFAULT_STOCK_SYMBOLS = []
    
    def __init__(
        self,
        signature: str,
        basemodel: str,
        stock_symbols: Optional[List[str]] = None,
        mcp_config: Optional[Dict[str, Dict[str, Any]]] = None,
        log_path: Optional[str] = None,
        max_steps: int = 5,
        max_retries: int = 3,
        base_delay: float = 0.5,
        openai_base_url: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        initial_cash: float = 500000.0,
        init_date: str = "2025-10-13",
        model_extra_body: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize BaseAgent
        
        Args:
            signature: Agent signature/name
            basemodel: Base model name
            stock_symbols: List of stock symbols, defaults to NASDAQ 100
            mcp_config: MCP tool configuration, including port and URL information
            log_path: Log path, defaults to ./data/agent_data
            max_steps: Maximum reasoning steps
            max_retries: Maximum retry attempts
            base_delay: Base delay time for retries
            openai_base_url: OpenAI API base URL
            openai_api_key: OpenAI API key
            initial_cash: Initial cash amount
            init_date: Initialization date
        """
        self.signature = signature
        self.basemodel = basemodel
        self.stock_symbols = stock_symbols or self.DEFAULT_STOCK_SYMBOLS
        self.max_steps = max_steps
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.initial_cash = initial_cash
        self.init_date = init_date
        
        # Set MCP configuration
        self.mcp_config = mcp_config or self._get_default_mcp_config()
        
        # Set log path
        self.base_log_path = log_path or "./data/agent_data"
        
        # Set OpenAI configuration
        if openai_base_url==None:
            self.openai_base_url = os.getenv("OPENAI_API_BASE")
        else:
            self.openai_base_url = openai_base_url
        if openai_api_key==None:
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
        else:
            self.openai_api_key = openai_api_key
        self.model_extra_body = model_extra_body
        
        # Initialize components
        self.client: Optional[MultiServerMCPClient] = None
        self.tools: Optional[List] = None
        self.model: Optional[ChatOpenAI] = None
        self.agent: Optional[Any] = None
        
        # Data paths
        self.data_path = os.path.join(self.base_log_path, self.signature)
        self.position_file = os.path.join(self.data_path, "position", "position.jsonl")
        
    def _get_default_mcp_config(self) -> Dict[str, Dict[str, Any]]:
        """Get default MCP configuration"""
        return {
            # "math": {
            #     "transport": "streamable_http",
            #     "url": f"http://localhost:{os.getenv('MATH_HTTP_PORT', '8000')}/mcp",
            # },
            # "stock_local": {
            #     "transport": "streamable_http",
            #     "url": f"http://localhost:{os.getenv('GETPRICE_HTTP_PORT', '8003')}/mcp",
            # },
            # "search": {
            #     "transport": "streamable_http",
            #     "url": f"http://localhost:{os.getenv('SEARCH_HTTP_PORT', '8001')}/mcp",
            # },
            "trade": {
                "transport": "streamable_http",
                "url": f"http://localhost:{os.getenv('TRADE_HTTP_PORT', '8002')}/mcp",
            },
            "analysis": {
                "transport": "streamable_http",
                "url": f"http://localhost:{os.getenv('ANALYSIS_HTTP_PORT', '8004')}/mcp",
            },
            "python": {
                "transport": "streamable_http",
                "url": f"http://localhost:{os.getenv('PYTHON_HTTP_PORT', '8005')}/mcp",
            },
            "news": {
                "transport": "streamable_http",
                "url": f"http://localhost:{os.getenv('NEWS_HTTP_PORT', '8006')}/mcp",
            },
            "macro": {
                "transport": "streamable_http",
                "url": f"http://localhost:{os.getenv('MACRO_HTTP_PORT', '8007')}/mcp",
            },
            "financial_report": {
                "transport": "streamable_http",
                "url": f"http://localhost:{os.getenv('FIN_REPORT_HTTP_PORT', '8008')}/mcp",
            },
        }
    
    async def initialize(self) -> None:
        """Initialize MCP client and AI model"""
        print(f"🚀 Initializing agent: {self.signature}")
        
        # Create MCP client
        self.client = MultiServerMCPClient(self.mcp_config)
        
        # Get tools
        try:
            self.tools = await self.client.get_tools()
            print(f"✅ Loaded {len(self.tools)} MCP tools")
        except BaseException as e:  # Catch ExceptionGroup as well on Python 3.11+
            print("❌ MCP 工具加载失败: ", str(e))
            # Try to expand nested exceptions if it's an ExceptionGroup-like object
            sub_errors = []
            try:
                # ExceptionGroup provides `.exceptions` attribute
                for idx, sub in enumerate(getattr(e, "exceptions", []) or []):
                    sub_errors.append(f"[{idx}] {type(sub).__name__}: {sub}")
            except Exception:
                pass
            if sub_errors:
                print("📋 子异常详情:")
                for line in sub_errors:
                    print("   ", line)
            print("🔧 MCP 配置检查:")
            for name, cfg in (self.mcp_config or {}).items():
                print(f"   - {name}: {cfg.get('url')}")
            raise
        
        # Create AI model
        if self.basemodel.startswith("deepseek-"):

            if self.basemodel == "deepseek-reasoner":
                max_tokens = 64000
            else:
                max_tokens = 16000
            model_kwargs: Dict[str, Any] = {}
            if self.model_extra_body:
                model_kwargs["extra_body"] = self.model_extra_body
            self.model = DeepseekReasonerWrapper(
                model=self.basemodel,
                api_key=self.openai_api_key or os.getenv("DEEPSEEK_API_KEY"),
                base_url=self.openai_base_url or os.getenv("DEEPSEEK_API_BASE"),
                max_retries=self.max_retries,
                timeout=600,
                max_tokens=max_tokens,
                temperature=0,
                model_kwargs=model_kwargs,
                #response_format=JSON_RESPONSE_FORMAT,
            )
        else:
            model_kwargs = {}
            if self.model_extra_body:
                model_kwargs["extra_body"] = self.model_extra_body
            self.model = ChatOpenAI(
                model=self.basemodel,
                base_url=self.openai_base_url,
                api_key=self.openai_api_key,
                max_retries=3,
                timeout=600,
                temperature=0,
                model_kwargs=model_kwargs,
                #response_format=JSON_RESPONSE_FORMAT,
            )
        
        # Note: agent will be created in run_trading_session() based on specific date
        # because system_prompt needs the current date and price information
        
        print(f"✅ Agent {self.signature} initialization completed")
    
    def _setup_logging(self, today_date: str) -> str:
        """Set up log file path"""
        log_path = os.path.join(self.base_log_path, self.signature, 'log', today_date)
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        return os.path.join(log_path, "log.jsonl")

    def _prepare_basic_info_payload(self, today_date: str) -> str:
        """Generate basic_stock_info snapshot for the tracked symbols."""
        try:
            payload = basic_info(
                TRACKED_SYMBOLS,
                today_time=today_date,
                price_lookback_days=DEFAULT_PRICE_LOOKBACK_DAYS,
                force_refresh=False,
                force_refresh_financials=False,
                use_cache=True,
            )
            return json.dumps(payload, ensure_ascii=False, indent=2)
        except Exception as exc:
            fallback = {
                "error": f"basic_info 生成失败: {exc}",
                "symbols": TRACKED_SYMBOLS,
            }
            print(f"⚠️ basic_stock_info 生成失败: {exc}")
            return json.dumps(fallback, ensure_ascii=False, indent=2)
    
    def _log_message(self, log_file: str, new_messages: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Log messages to log file"""
        # 统一为列表，便于后续处理
        if isinstance(new_messages, dict):
            normalized_messages: List[Dict[str, Any]] = [new_messages]
        elif isinstance(new_messages, list):
            normalized_messages = new_messages
        else:
            raise TypeError("new_messages 必须是 dict 或 list[dict]")

        # 处理new_messages中的每个消息，如果包含不可序列化的对象，则转换为可序列化格式
        serializable_messages = []
        for msg in normalized_messages:
            msg_copy: Dict[str, Any] = deepcopy(msg)
            # 如果消息中包含raw_response且是字典类型，进一步处理
            if 'raw_response' in msg_copy and isinstance(msg_copy['raw_response'], dict):
                raw_response = msg_copy['raw_response']
                # 检查是否包含messages键，且messages是列表
                if 'messages' in raw_response and isinstance(raw_response['messages'], list):
                    # 创建一个新的messages列表，处理其中的每个消息对象
                    processed_messages = []
                    for message_obj in raw_response['messages']:
                        # 如果消息对象有to_json方法（如LangChain的消息对象），则使用它
                        if hasattr(message_obj, 'to_json'):
                            processed_messages.append(message_obj.to_json())
                        # 否则，尝试提取常见的属性
                        elif hasattr(message_obj, '__dict__'):
                            # 提取对象的属性字典，并尝试处理其中的特殊属性
                            obj_dict = message_obj.__dict__.copy()
                            # 如果有content属性且是列表（如包含文本和图像的复杂内容），保持原样
                            # 如果content是简单类型，也保持原样
                            processed_messages.append(obj_dict)
                        else:
                            # 如果既没有to_json方法也没有__dict__，则转换为字符串
                            processed_messages.append(str(message_obj))
                    # 更新raw_response中的messages
                    raw_response['messages'] = processed_messages
                    # 更新消息中的raw_response
                    msg_copy['raw_response'] = raw_response
            # 为日志补充多行可读内容（不影响发送给AI的原始content）
            content_val = msg_copy.get('content')
            if isinstance(content_val, str):
                msg_copy['content_lines'] = content_val.split('\n')
            # 将处理后的消息添加到可序列化消息列表中
            serializable_messages.append(msg_copy)

        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "signature": self.signature,
            "new_messages": serializable_messages
        }
        # 使用indent参数提高JSON可读性，并添加分隔线便于区分不同日志条目
        formatted_json = json.dumps(log_entry, ensure_ascii=False, indent=2)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(formatted_json + "\n")
            f.write("-" * 80 + "\n")  # 添加分隔线

    def _persist_ai_output_snapshot(
        self,
        today_date: str,
        *,
        raw_json: str,
        payload: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """保存AI最终输出（原始/解析后）到本地，便于追溯。"""

        snapshot_dir = Path(self.base_log_path) / self.signature / "ai_outputs"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        record = {
            "date": today_date,
            "timestamp": datetime.now().isoformat(),
            "raw_json": raw_json,
            "payload": payload,
            "error": error,
        }
        snapshot_path = snapshot_dir / f"{today_date}_{timestamp}.json"
        snapshot_path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    
    async def _ainvoke_with_retry(self, message: List[Dict[str, str]]) -> Any:
        """Agent invocation with retry"""
        for attempt in range(1, self.max_retries + 1):
            try:
                return await self.agent.ainvoke(
                    {"messages": message}, 
                    {"recursion_limit": 100}
                )
            except Exception as e:
                if attempt == self.max_retries:
                    raise e
                print(f"⚠️ Attempt {attempt} failed, retrying after {self.base_delay * attempt} seconds...")
                print(f"Error details: {e}")
                await asyncio.sleep(self.base_delay * attempt)
    
    def _save_agent_response(self, today_date: str, agent_response: Any) -> None:
        """Save agent response (raw object) to local file for debugging."""
        # Create directory for saving agent responses
        agent_responses_dir = os.path.join(self.data_path, "agent_responses")
        if not os.path.exists(agent_responses_dir):
            os.makedirs(agent_responses_dir)
            
        # Define file path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"agent_response_{today_date}_{timestamp}.txt"
        file_path = os.path.join(agent_responses_dir, filename)
        
        if isinstance(agent_response, (dict, list)):
            payload = json.dumps(agent_response, ensure_ascii=False, indent=2)
        else:
            payload = str(agent_response)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(payload)
            print(f"💾 Agent response saved to: {file_path}")
        except Exception as e:
            print(f"❌ Failed to save agent response: {e}")

    def _finalize_payload(self, today_date: str, raw_json_str: str) -> None:
        try:
            payload = json.loads(raw_json_str)
            self._persist_ai_output_snapshot(
                today_date,
                raw_json=raw_json_str,
                payload=payload,
            )
            saved_ops = save_daily_operations(self.signature, payload)
            process_and_merge_operations(self.signature, saved_ops)
            print(f"📝 已保存并合并 {today_date} 的每日操盘总结")
        except Exception as exc:
            self._persist_ai_output_snapshot(
                today_date,
                raw_json=raw_json_str,
                error=str(exc),
            )
            print(f"❌ JSON解析或保存失败(致命错误): {exc}")
            sys.exit(1)

    async def run_trading_session(self, today_date: str) -> None:
        """
        Run single day trading session
        
        Args:
            today_date: Trading date
        """
        print(f"📈 Starting trading session: {today_date}")
        
        # Set up logging
        log_file = self._setup_logging(today_date)
        
        # Update system prompt
        sys_prompt = get_agent_system_prompt(today_date, self.signature)
        self.agent = create_agent(
            self.model,
            tools=self.tools,
            system_prompt= sys_prompt,
        )
        # Log system prompt for auditing
        self._log_message(
            log_file,
            [
                {
                    "role": "system",
                    "content": sys_prompt,
                }
            ],
        )
        
        # Initial user query
        # 需要把昨天的股票信息给AI，防止AI看到今天的最新信息
        basic_info_day = (datetime.strptime(today_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        basic_snapshot = self._prepare_basic_info_payload(basic_info_day)
        user_query = [
            {
                "role": "user",
                "content": (
                    f"今天是 {today_date} 早上，股市还没开盘 \n"
                    "以下为昨天最新的股池中所有股票的《基本面数据概览》：\n"
                    f"{basic_snapshot}"
                    "请遵循system_prompt的规范和价值投资原则，调用工具，获取感兴趣股票的最新消息宏观环境、基本面数据和市场动态，分析并决策今日的持仓调整。买入卖出价格以《基本面数据概览》中每只股票的latest_price为准，这是昨天收盘价。\n"
                ),
            }
        ]
        message = user_query.copy()
        
        # Log initial message
        self._log_message(log_file, user_query)
        
        # Trading loop
        current_step = 0
        self.max_steps = 1
        while current_step < self.max_steps:
            current_step += 1
            print(f"🔄 Step {current_step}/{self.max_steps}")

            try:
                # Call agent
                response = await self._ainvoke_with_retry(message)

                # breakpoint()
                # Extract agent response
                agent_response = extract_conversation(response, "final")
                reasoning_trace = extract_reasoning_details(response)

                # Save agent response to local file
               # self._save_agent_response(today_date, response)

                assistant_msg = {"role": "assistant", "content": agent_response}
                if reasoning_trace:
                    assistant_msg["reasoning_trace"] = reasoning_trace
                message.append(assistant_msg)
                self._log_message(
                    log_file,
                    [{
                        "role": "assistant",
                        "raw_response": response,
                        "content": agent_response,
                        "reasoning_trace": reasoning_trace,
                    }],
                )

                if STOP_SIGNAL in agent_response:
                    raw_json_str = extract_json_from_ai_output(agent_response)
                    if not raw_json_str:
                        print("⚠️ 未检测到 json 标签，程序终止。")
                        sys.exit(1)
                    self._finalize_payload(today_date, raw_json_str)
                    break

                raw_json_str = extract_json_from_ai_output(agent_response)
                if not raw_json_str:
                    print("⚠️ JSON 解析失败，程序终止。")
                    sys.exit(1)
                self._finalize_payload(today_date, raw_json_str)
                break
                
            except Exception as e:
                print(f"❌ Trading session error: {str(e)}")
                print(f"Error details: {e}")
                raise
        
        # Handle trading results
        await self._handle_trading_result(today_date)
    
    async def _handle_trading_result(self, today_date: str) -> None:
        """Handle trading results"""
        if_trade = get_config_value("IF_TRADE")
        if if_trade:
            write_config_value("IF_TRADE", False)
            print("✅ Trading completed")
        else:
            print("📊 No trading, maintaining positions")
            try:
                add_no_trade_record(today_date, self.signature)
            except NameError as e:
                print(f"❌ NameError: {e}")
                raise
            write_config_value("IF_TRADE", False)
    
    def register_agent(self) -> None:
        """Register new agent, create initial positions"""
        # Check if position.jsonl file already exists
        if os.path.exists(self.position_file):
            print(f"⚠️ Position file {self.position_file} already exists, skipping registration")
            return
        
        # Ensure directory structure exists
        position_dir = os.path.join(self.data_path, "position")
        if not os.path.exists(position_dir):
            os.makedirs(position_dir)
            print(f"📁 Created position directory: {position_dir}")
        
        # Create initial positions
        init_position = {symbol: 0 for symbol in self.stock_symbols}
        init_position['CASH'] = self.initial_cash
        
        with open(self.position_file, "w") as f:  # Use "w" mode to ensure creating new file
            # 初始记录增加 total_value 字段（等于初始现金）
            f.write(json.dumps({
                "date": self.init_date, 
                "id": 0, 
                "positions": init_position,
                "this_action": {"action": "init"},
                "total_value": init_position.get("CASH", 0.0)
            }) + "\n")
        
        print(f"✅ Agent {self.signature} registration completed")
        print(f"📁 Position file: {self.position_file}")
        print(f"💰 Initial cash: ${self.initial_cash}")
        print(f"📊 Number of stocks: {len(self.stock_symbols)}")
    

    def get_trading_dates(self, init_date: str, end_date: str, force_run: bool = False) -> List[str]:
        """
        Get trading date list
        
        Args:
            init_date: Start date
            end_date: End date
            force_run: Whether to force run from init_date
            
        Returns:
            List of trading dates
        """
        # Ensure agent is registered and position file exists
        if not os.path.exists(self.position_file):
            self.register_agent()

        if force_run:
            start_date_obj = datetime.strptime(init_date, "%Y-%m-%d")
        else:
            dates = []
            max_date = None
            has_traded_on_max_date = False  # 标记最大日期是否已经交易过
            
            # Position file guaranteed to exist now due to check above, but logic remains same
            # Read existing position file, find latest date and check if it has trades
            with open(self.position_file, "r") as f:
                for line in f:
                    doc = json.loads(line)
                    current_date = doc.get('date')
                    # Skip invalid or missing date entries
                    if not isinstance(current_date, str) or not current_date.strip():
                        continue
                    
                    # Check if this date has trading action
                    this_action = doc.get("this_action")
                    has_action = this_action is not None and this_action.get("action") != "init"
                    
                    if max_date is None:
                        max_date = current_date.strip()
                        has_traded_on_max_date = has_action
                    else:
                        current_date_obj = datetime.strptime(current_date.strip(), "%Y-%m-%d")
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if current_date_obj > max_date_obj:
                            max_date = current_date.strip()
                            has_traded_on_max_date = has_action
                        elif current_date_obj == max_date_obj:
                            # If same date, update has_traded flag if this record has action
                            if has_action:
                                has_traded_on_max_date = True

            # If no valid date was found in the position file, fall back to init_date
            if max_date is None:
                max_date = init_date
            
            # Determine the start date for trading
            max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
            
            # If the max date has already been traded, start from the next day
            if has_traded_on_max_date:
                # Get the next trading day after max_date
                start_date_obj = max_date_obj + timedelta(days=1)
            else:
                # If max date has no trades, start from max_date
                start_date_obj = max_date_obj
        
        # Check if new dates need to be processed
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        if end_date_obj < start_date_obj:
            return []
        
        # 通过parse_symbol获取市场日历标识，并映射到pandas_market_calendars
        # 示例: 'CN' / 'HK' / 'US'
        market_tag = parse_symbol(self.stock_symbols[0]).calendar
        # 与 `utlity.stock_utils` 保持一致的交易所名称映射
        calendar_name_map = {
            "US": "NYSE",
            "CN": "SSE",
            "HK": "HKEX",
        }
        mcal_name = calendar_name_map.get(market_tag, "NYSE")
        market_calendar = mcal.get_calendar(mcal_name)

        # 使用valid_days获取有效交易日，并转换为YYYY-MM-DD字符串列表
        trading_days_ts = market_calendar.valid_days(start_date=start_date_obj, end_date=end_date_obj)
        trading_dates = [ts.strftime("%Y-%m-%d") for ts in trading_days_ts]
        
        return trading_dates
    
    async def run_with_retry(self, today_date: str) -> None:
        """Run method with retry"""
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"🔄 Attempting to run {self.signature} - {today_date} (Attempt {attempt})")
                await self.run_trading_session(today_date)
                print(f"✅ {self.signature} - {today_date} run successful")
                return
            except Exception as e:
                print(f"❌ Attempt {attempt} failed: {str(e)}")
                if attempt == self.max_retries:
                    print(f"💥 {self.signature} - {today_date} all retries failed")
                    raise
                else:
                    wait_time = self.base_delay * attempt
                    print(f"⏳ Waiting {wait_time} seconds before retry...")
                    await asyncio.sleep(wait_time)
    
    async def run_date_range(self, init_date: str, end_date: str, force_run: bool = False) -> None:
        """
        Run all trading days in date range
        
        Args:
            init_date: Start date
            end_date: End date
            force_run: Whether to force run from init_date
        """
        # 使用logger记录信息，不使用print
        print(f"📅 运行日期范围:{init_date} 到 {end_date}")

        # 初始化每日操盘总结存储结构（按 signature）
        try:
            initialize_data_files(self.signature)
        except Exception as e:
            print(f"⚠️ 数据目录初始化失败: {e}")
            
        trading_dates = self.get_trading_dates(init_date, end_date, force_run=force_run)

        if not trading_dates:
            print("ℹ️ 无需处理的交易日")
            return

        print(f"📊 待处理交易日数量: {len(trading_dates)}")

        # 逐日执行交易流程
        for date_str in trading_dates:
            print(f"🔄 处理 {self.signature} - 日期: {date_str}")

            # 写入配置供下游流程使用
            write_config_value("TODAY_DATE", date_str)
            write_config_value("SIGNATURE", self.signature)

            # 简洁调用，不额外包装异常处理
            await self.run_with_retry(date_str)

        print(f"✅ {self.signature} 处理完成")
    
    def get_position_summary(self) -> Dict[str, Any]:
        """Get position summary"""
        if not os.path.exists(self.position_file):
            return {"error": "Position file does not exist"}
        
        positions = []
        with open(self.position_file, "r") as f:
            for line in f:
                positions.append(json.loads(line))
        
        if not positions:
            return {"error": "No position records"}
        
        latest_position = positions[-1]
        return {
            "signature": self.signature,
            "latest_date": latest_position.get("date"),
            "positions": latest_position.get("positions", {}),
            "total_records": len(positions)
        }
    
    def __str__(self) -> str:
        return f"BaseAgent(signature='{self.signature}', basemodel='{self.basemodel}', stocks={len(self.stock_symbols)})"
    
    def __repr__(self) -> str:
        return self.__str__()



