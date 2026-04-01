from __future__ import annotations

import json
import logging
import re

import anthropic

logger = logging.getLogger("trading.analysis.sonnet")

SYSTEM_PROMPT = """你是一位专业的A股超短线交易分析师，专注于涨停板生态和短线题材轮动分析。

你的任务是：
1. 从今日财经新闻中识别2-4个最强热点题材
2. 将候选股票与热点题材进行匹配
3. 为每只候选股打分（1-10分），评分维度包括：题材强度、量价配合、资金关注度、连板高度

你必须以严格的JSON格式输出结果，不要在JSON之外包含任何文本。"""


class SonnetAnalyzer:
    def __init__(self, config: dict):
        self.client = anthropic.Anthropic(timeout=120.0)
        ai_cfg = config.get("ai", {})
        self.model = ai_cfg.get("sonnet_model", "claude-sonnet-4-5-20241022")
        self.max_tokens = ai_cfg.get("sonnet_max_tokens", 4096)
        self.temperature = ai_cfg.get("temperature", 0.3)

    def analyze(self, market_data: dict, candidates: list[dict]) -> dict:
        if not candidates:
            logger.warning("无候选股，跳过 Sonnet 分析")
            return self._empty_result()

        prompt = self._build_prompt(market_data, candidates)
        logger.info("调用 Sonnet 分析 %d 只候选股...", len(candidates))

        for attempt in range(2):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                result = self._parse_json(text)
                if result:
                    logger.info("Sonnet 分析完成，识别 %d 个热点题材",
                                len(result.get("hot_topics", [])))
                    return result
                logger.warning("Sonnet 输出 JSON 解析失败 (尝试 %d/2)", attempt + 1)
            except Exception as e:
                logger.warning("Sonnet 调用失败 (尝试 %d/2): %s", attempt + 1, e)

        logger.error("Sonnet 分析失败，返回空结果")
        return self._empty_result()

    def _build_prompt(self, market_data: dict, candidates: list[dict]) -> str:
        date = market_data.get("date", "")
        indices = market_data.get("indices", {})
        overnight = market_data.get("overnight", {})
        northbound = market_data.get("northbound", {})
        margin = market_data.get("margin", {})
        sentiment = market_data.get("sentiment", {})
        news = market_data.get("news_headlines", [])

        # 龙虎榜摘要
        dt = market_data.get("dragon_tiger")
        dt_summary = "无数据"
        if dt is not None and not dt.empty:
            dt_lines = []
            for _, row in dt.head(10).iterrows():
                name = row.get("名称", row.get("name", ""))
                reason = row.get("上榜原因", row.get("reason", ""))
                dt_lines.append(f"  - {name}: {reason}")
            dt_summary = "\n".join(dt_lines)

        # 候选股表格
        cand_lines = []
        for c in candidates:
            line = (
                f"  - {c['code']} {c['name']} | "
                f"涨跌幅:{c.get('change_pct', 'N/A')}% | "
                f"量比:{c.get('volume_ratio', 'N/A')} | "
                f"换手率:{c.get('turnover_rate', 'N/A')}% | "
                f"连板:{c.get('consecutive_boards', 0)} | "
                f"行业:{c.get('industry', 'N/A')} | "
                f"龙虎榜:{'是' if c.get('on_dragon_tiger') else '否'} | "
                f"技术形态:{c.get('technical_note', '无')}"
            )
            cand_lines.append(line)

        news_text = "\n".join(f"  - {h}" for h in news[:25]) if news else "  无新闻数据"

        return f"""## 今日市场数据 ({date})

### 大盘表现
- 上证指数：{indices.get('sh_index_close', 'N/A')}（{indices.get('sh_index_change', 'N/A')}%）
- 深证成指：{indices.get('sz_index_close', 'N/A')}（{indices.get('sz_index_change', 'N/A')}%）
- 创业板指：{indices.get('cyb_index_close', 'N/A')}（{indices.get('cyb_index_change', 'N/A')}%）

### 隔夜外盘
- 标普500：{overnight.get('us_sp500_change', 'N/A')}%
- 纳斯达克：{overnight.get('us_nasdaq_change', 'N/A')}%
- 恒生指数：{overnight.get('hk_hsi_change', 'N/A')}%

### 资金面
- 北向资金净流入：{northbound.get('northbound_net', 'N/A')}亿
- 融资余额：{margin.get('margin_balance', 'N/A')}亿

### 市场情绪
- 昨日涨停数：{sentiment.get('limit_up_count', 'N/A')}
- 昨日跌停数：{sentiment.get('limit_down_count', 'N/A')}
- 炸板率：{sentiment.get('failed_limit_rate', 'N/A')}%

### 今日财经新闻
{news_text}

### 昨日龙虎榜要点
{dt_summary}

### 候选股列表（共{len(candidates)}只）
{chr(10).join(cand_lines)}

请输出JSON，格式如下：
{{
  "hot_topics": [
    {{"topic": "题材名", "catalyst": "催化剂", "strength": "强/中/弱", "reasoning": "分析逻辑"}}
  ],
  "market_score": 7,
  "market_sentiment_summary": "一句话市场情绪总结",
  "scored_candidates": [
    {{
      "code": "000001",
      "name": "示例股票",
      "score": 8.5,
      "matched_theme": "题材名",
      "analysis": "推荐逻辑（2-3句话）",
      "risk": "主要风险点"
    }}
  ]
}}"""

    def _parse_json(self, text: str) -> "dict | None":
        # 直接解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 提取 ```json ... ``` 代码块
        match = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # 提取第一个 { ... } 块
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        return None

    def _empty_result(self) -> dict:
        return {
            "hot_topics": [],
            "market_score": 5,
            "market_sentiment_summary": "分析数据不足",
            "scored_candidates": [],
        }
