from utlity.stock_utils import SymbolInfo
import csv
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import List

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel
from utlity.stock_utils import resolve_base_dir, SymbolInfo

load_dotenv()

DEEPSEEK_MODEL = (
    os.getenv("DEEPSEEK_SIMILAR_STOCK_MODEL")
    or os.getenv("DEEPSEEK_MODEL")
    or "deepseek-chat"
)
DEEPSEEK_TIMEOUT = int(os.getenv("DEEPSEEK_TIMEOUT", "60"))

class SimilarStock(BaseModel):
    """相似股票数据模型"""
    similar_stock_name: str
    similar_stock_code: str
    reasons_for_selecting_similar_stocks: str


def _call_deepseek_chat(prompt: str, model: str | None = None) -> str:
    """调用 DeepSeek Chat Completion 接口并返回纯文本内容。"""
    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "未找到 DeepSeek API Key，请在环境变量 DEEPSEEK_API_KEY 或 OPENAI_API_KEY 中配置。"
        )

    api_base = (
        os.getenv("DEEPSEEK_API_BASE")
        or os.getenv("OPENAI_API_BASE")
        or "https://api.deepseek.com"
    )
    url = api_base.rstrip("/") + "/chat/completions"

    payload = {
        "model": model or DEEPSEEK_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是一名专注于A股与港股的资深股票分析师，"
                    "回答时严格遵循用户的格式要求。"
                ),
            },
            {"role": "user", "content": prompt.strip()},
        ],
        "temperature": 0.4,
        "top_p": 0.9,
        "max_tokens": 8192,
    }

    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=DEEPSEEK_TIMEOUT) as response:
            response_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"DeepSeek API 请求失败 (HTTP {exc.code}): {error_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"无法连接到 DeepSeek API: {exc.reason}") from exc

    try:
        response_json = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"解析 DeepSeek 响应失败: {exc}") from exc

    choices = response_json.get("choices") or []
    if not choices:
        raise RuntimeError(f"DeepSeek API 返回内容为空: {response_json}")

    message = choices[0].get("message") or {}
    content = message.get("content")
    if not content:
        raise RuntimeError("DeepSeek API 响应中缺少内容")

    return content

def extract_similar_stocks_from_json(json_data):
    """从JSON数据中提取相似股票信息"""
    similar_stocks = []
    try:
        # 如果json_data已经是字典，直接使用
        if isinstance(json_data, list):
            data_list = json_data
        else:
            # 尝试解析JSON字符串
            data_list = json.loads(json_data)
        
        for item in data_list:
            similar_stocks.append({
                'name': item['similar_stock_name'],
                'code': item['similar_stock_code'],
                'reason': item['reasons_for_selecting_similar_stocks']
            })
    except Exception as e:
        print(f"解析JSON数据时出错: {e}")
    
    return similar_stocks

def extract_similar_stocks_from_text(response_text):
    """从文本响应中提取相似股票信息"""
    # 首先移除尾部的通用提示文本
    cleanup_patterns = [
        r'\n\s*请注意，股票市场具有波动性.*?(?:仅供参考)\.?',
        r'\n\s*以上分析基于.*?(?:仅供参考)\.?',
        r'\n\s*注：.*?(?:仅供参考)\.?'
    ]
    
    for pattern in cleanup_patterns:
        response_text = re.sub(pattern, '', response_text, flags=re.DOTALL)
    
    # 方法1: 尝试匹配"股票名称: xxx\n股票代码: xxx\n相关原因: xxx"模式（包括可能的加粗格式）
    pattern1 = r'(?:\*\*)?(?:股票名称|公司名称)(?:\*\*)?\s*[:：]\s*(.*?)\s*\n(?:\*\*)?(?:股票代码|代码)(?:\*\*)?\s*[:：]\s*(.*?)\s*\n(?:\*\*)?(?:相关原因|相关性|关联性|原因)(?:\*\*)?\s*[:：]\s*(.*?)(?=\n(?:\*\*)?(?:股票名称|公司名称)|$)'
    matches1 = re.findall(pattern1, response_text, re.DOTALL)
    
    if matches1:
        similar_stocks = []
        for match in matches1:
            name, code, reason = match
            similar_stocks.append({
                'name': name.strip(),
                'code': code.strip(),
                'reason': reason.strip()
            })
        return similar_stocks
    
    # 方法2: 尝试匹配"[股票名称] - [股票代码] - 相关原因"模式（包括可能的加粗格式）
    pattern2 = r'\[(.*?)\]\s*-\s*\[(.*?)\]\s*-\s*(.*?)(?=\n\[|$)'
    matches2 = re.findall(pattern2, response_text, re.DOTALL)
    
    if matches2:
        similar_stocks = []
        for match in matches2:
            name, code, reason = match
            similar_stocks.append({
                'name': name.strip(),
                'code': code.strip(),
                'reason': reason.strip()
            })
        return similar_stocks
    
    # 方法3: 尝试匹配"[股票名称]（股票代码）：与xxx的相关性原因"模式
    pattern3 = r'\[(.*?)\](?:（|\()(.*?)(?:）|\))[:：](.*?)(?=\n\[|$)'
    matches3 = re.findall(pattern3, response_text, re.DOTALL)
    
    if matches3:
        similar_stocks = []
        for match in matches3:
            name, code, reason = match
            similar_stocks.append({
                'name': name.strip(),
                'code': code.strip(),
                'reason': reason.strip()
            })
        return similar_stocks
    
    # 方法4: 匹配数字编号格式："1. 股票名称 (股票代码)\n   相关原因: xxx"
    pattern4 = r'(\d+)\.\s+(.*?)\s+\((.*?)\)[^\n]*\n\s*相关原因:\s*(.*?)(?=\n\d+\.|$)'
    matches4 = re.findall(pattern4, response_text, re.DOTALL)
    
    if matches4:
        similar_stocks = []
        for match in matches4:
            _, name, code, reason = match
            similar_stocks.append({
                'name': name.strip(),
                'code': code.strip(),
                'reason': reason.strip()
            })
        return similar_stocks
    
    # 方法5: 尝试匹配股票名称和代码的其他格式（包括可能的加粗格式）
    # 首先尝试匹配所有可能包含"股票名称"和"股票代码"的行
    name_pattern = r'(?:\*\*)?(?:股票名称|公司名称|名称)(?:\*\*)?\s*[:：]\s*(.*?)(?:\n|$)'
    code_pattern = r'(?:\*\*)?(?:股票代码|代码)(?:\*\*)?\s*[:：]\s*(.*?)(?:\n|$)'
    reason_pattern = r'(?:\*\*)?(?:相关原因|相关性|关联性|原因)(?:\*\*)?\s*[:：]\s*(.*?)(?=\n(?:\*\*)?(?:股票名称|公司名称|名称)|$)'
    
    names = re.findall(name_pattern, response_text)
    codes = re.findall(code_pattern, response_text)
    reasons = re.findall(reason_pattern, response_text, re.DOTALL)
    
    if names and codes and len(names) == len(codes):
        similar_stocks = []
        for i in range(min(len(names), 5)):  # 最多取5个
            name = names[i].strip()
            code = codes[i].strip()
            reason = reasons[i].strip() if i < len(reasons) else ""
            similar_stocks.append({
                'name': name,
                'code': code,
                'reason': reason
            })
        return similar_stocks
    
    # 如果以上方法都失败，返回空列表
    print("无法从响应中提取股票信息，请检查响应格式")
    return []

def save_to_csv(target_stock_name, target_stock_code, similar_stocks, filename: str | Path | None = None, base_dir: Path | str | None = None):
    """将相似股票信息保存到CSV文件

    当未指定 filename 时，默认写入到 resolve_base_dir(base_dir)/global_cache/similar_stocks.csv。
    会在保存前确保父目录存在。
    """
    # 解析保存路径
    if filename is None:
        path = resolve_base_dir(base_dir) / 'global_cache' / 'similar_stocks.csv'
    else:
        path = Path(filename)

    # 确保目录存在
    path.parent.mkdir(parents=True, exist_ok=True)

    # 检查文件是否存在
    file_exists = path.exists()
    
    # 打开文件并写入数据
    with open(path, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['target_stock_name', 'target_stock_code', 
                     'similar_stock1_name', 'similar_stock1_code', 'similar_stock1_reason',
                     'similar_stock2_name', 'similar_stock2_code', 'similar_stock2_reason',
                     'similar_stock3_name', 'similar_stock3_code', 'similar_stock3_reason',
                     'similar_stock4_name', 'similar_stock4_code', 'similar_stock4_reason',
                     'similar_stock5_name', 'similar_stock5_code', 'similar_stock5_reason']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        
        # 如果文件不存在，写入表头
        if not file_exists:
            writer.writeheader()
        
        # 准备数据行
        row_data = {
            'target_stock_name': target_stock_name,
            'target_stock_code': target_stock_code
        }
        
        # 添加相似股票数据
        for i, stock in enumerate(similar_stocks, 1):
            if i <= 5:  # 最多保存5只相似股票
                # 清理数据中的特殊字符，避免CSV解析问题
                name = str(stock['name']).replace('\n', ' ').replace('\r', ' ').strip()
                code = str(stock['code']).replace('\n', ' ').replace('\r', ' ').strip()
                reason = str(stock['reason']).replace('\n', ' ').replace('\r', ' ').strip()
                
                row_data[f'similar_stock{i}_name'] = name
                row_data[f'similar_stock{i}_code'] = code
                row_data[f'similar_stock{i}_reason'] = reason
        
        # 写入数据
        writer.writerow(row_data)
    
    print(f"相似股票数据已保存到 {path}")

def get_similar_stocks(symbolInfo: SymbolInfo, base_dir: Path | str | None = None): 
    """获取与指定股票相似的股票"""
    # 先检查CSV文件中是否已有该股票的数据
    similar_stocks_path = resolve_base_dir(base_dir) / 'global_cache' / 'similar_stocks.csv'
    stock_code = symbolInfo.code
    stock_name = symbolInfo.stock_name
    if similar_stocks_path.is_file():
        # 确保所有股票代码列都为字符串类型
        dtype_dict = {'target_stock_code': str}
        for i in range(1, 6):
            dtype_dict[f'similar_stock{i}_code'] = str
        df = pd.read_csv(similar_stocks_path, dtype=dtype_dict)
        
        existing_data = df[df['target_stock_code'] == stock_code]
        
        if not existing_data.empty:
            print(f"从CSV文件中获取{stock_name}的相似股票数据...")
            similar_stocks = []
            row = existing_data.iloc[0]
            for i in range(1, 6):
                name_col = f'similar_stock{i}_name'
                code_col = f'similar_stock{i}_code'
                reason_col = f'similar_stock{i}_reason'
                if pd.notna(row[name_col]) and pd.notna(row[code_col]):
                    similar_stocks.append({
                        'name': str(row[name_col]),
                        'code': str(row[code_col]),  # 确保代码为字符串类型
                        'reason': str(row[reason_col]) if pd.notna(row[reason_col]) else ''
                    })
            return similar_stocks
    
    # 如果CSV中没有，则通过API获取
    try:
        prompt = f"""
        我想对{stock_name}(股票代码{stock_code})的股票，指标等进行分析。所以我想找到和{stock_name}业务最相关的5只股票进行联合分析。
        请帮助我找出和{stock_name}（股票代码{stock_code}）业务最相似的5只A股或港股股票，即同行业的5只股票，注意只关注A股和港股，要求必须是同行业的最相似的股票。
        
        注意：不能包含输入的股票本身作为相似股票，草泥马的听不懂人话就去死
        请考虑以下因素：
        1、在技术领域有高度竞争同领域公司
        2、与{stock_name}在市场上有高度相关性即高度相似的公司
        
        最终请严格按照如下格式返回与{stock_name}业务最相关的5只股票，每只股票依次包含这三个字段，并且请确保信息的准确性:

        **股票名称**: 第一只相似股票名称
        **股票代码**: 第一只相似股票代码
        **相关原因**: 第一只股票与{stock_name}的相关性原因

        **股票名称**: 第二只相似股票名称
        **股票代码**: 第二只相似股票代码
        **相关原因**: 第二只股票与{stock_name}的相关性原因

        以此类推，共5只股票
        注意：输出的股票名称写主流如同花顺、东方财富上的股票名称，股票代码港股的以.HK结尾，A股以.SZ或.SH结尾。最相关的放到越前面输出，只关注A股和港股，不要美股
        """

        # 发送请求并等待响应 - 使用重试机制
        max_retries = 3
        response_text = ""
        
        for attempt in range(max_retries):
            try:
                print(f"尝试获取相似股票数据 (第{attempt + 1}次)...")
                response_text = _call_deepseek_chat(prompt)
                print(response_text)
                break  # 成功获取响应，跳出重试循环
                
            except Exception as retry_error:
                print(f"第{attempt + 1}次尝试失败: {retry_error}")
                if attempt == max_retries - 1:
                    print("所有重试都失败，无法获取相似股票数据")
                    return []
                else:
                    print("等待5秒后重试...")
                    time.sleep(5)

        if not response_text:
            print("DeepSeek 没有返回任何内容，结束本次请求。")
            return []
        
        # 从响应中提取相似股票数据
        similar_stocks = extract_similar_stocks_from_text(response_text)
        
        if not similar_stocks:
            print("未能成功提取股票信息，请检查响应格式或调整正则表达式")
        else:
            print(f"成功提取出{len(similar_stocks)}只相似股票信息")
        
        # 保存到CSV（确保保存到全局缓存路径）
        save_to_csv(stock_name, stock_code, similar_stocks, filename=similar_stocks_path)
        
        return similar_stocks

    except Exception as e:
        print(f"发生错误: {e}")
        return []

def query_similar_stocks(target_stock=None, base_dir: Path | str | None = None):
    """查询已保存的相似股票数据
    
    参数:
        target_stock: 目标股票名称或代码，如果为None则返回所有数据
    
    返回:
        DataFrame形式的相似股票数据
    """
    csv_path = resolve_base_dir(base_dir) / 'global_cache' / 'similar_stocks.csv'
    if not csv_path.is_file():
        print("没有找到类似股票缓存文件，请先运行获取相似股票的功能")
        return None
    
    df = pd.read_csv(csv_path)  # quoting=1 表示 QUOTE_ALL
    
    if target_stock is None:
        return df
    
    # 按股票名称或代码查询
    result = df[(df['target_stock_name'] == target_stock) | (df['target_stock_code'] == target_stock)]
    
    if result.empty:
        print(f"没有找到与'{target_stock}'相关的数据")
        return None
    
    return result

def print_similar_stocks(df):
    """打印相似股票数据表格"""
    if df is None or df.empty:
        return
    
    for _, row in df.iterrows():
        print(f"\n目标股票: {row['target_stock_name']} ({row['target_stock_code']})")
        print("相似股票:")
        
        for i in range(1, 6):
            name_col = f'similar_stock{i}_name'
            code_col = f'similar_stock{i}_code'
            reason_col = f'similar_stock{i}_reason'
            
            if pd.notna(row[name_col]) and pd.notna(row[code_col]):
                reason = row[reason_col] if pd.notna(row[reason_col]) else "无"
                print(f"  {i}. {row[name_col]} ({row[code_col]})")
                print(f"     相关原因: {reason}")

# 示例用法
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='获取与目标股票相似的股票')
    parser.add_argument('--name', type=str, help='目标股票名称')
    parser.add_argument('--code', type=str, help='目标股票代码')
    parser.add_argument('--query', type=str, help='查询已保存的相似股票数据')
    parser.add_argument('--list-all', action='store_true', help='列出所有已保存的相似股票数据')
    
    args = parser.parse_args()
    
    if args.list_all:
        # 列出所有已保存的数据
        df = query_similar_stocks()
        print_similar_stocks(df)
    elif args.query:
        # 查询特定股票的相似股票
        df = query_similar_stocks(args.query)
        print_similar_stocks(df)
    elif args.name and args.code:
        # 获取新的相似股票数据
        similar_stocks = get_similar_stocks(args.name, args.code)
        print("\n提取到的相似股票:")
        for i, stock in enumerate(similar_stocks, 1):
            print(f"{i}. {stock['name']} ({stock['code']})")
            print(f"   相关原因: {stock['reason']}")
    else:
        # 默认使用海康威视作为示例
        similar_stocks = get_similar_stocks("海康威视", "002415")
        print("\n提取到的相似股票:")
        for i, stock in enumerate(similar_stocks, 1):
            print(f"{i}. {stock['name']} ({stock['code']})")
            print(f"   相关原因: {stock['reason']}")
