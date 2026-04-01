from __future__ import annotations

import json
import logging
import re

import anthropic

logger = logging.getLogger("trading.analysis.opus")

SYSTEM_PROMPT = """你是一位资深A股超短线交易策略师，拥有20年短线交易经验。
你需要从分析师的初步分析中做最终决策，精选4只个股加入今日观察名单。

重要：这是观察名单，不是直接买入推荐。用户会在开盘后9:30-10:00观察走势，确认强势后才入场。

决策原则：
1. 优先选择首板或二板股（1-2连板最优，风险收益比最好）
2. 题材必须有持续性催化（政策、事件、资金验证）
3. 量价配合是核心：放量突破优于缩量上涨
4. 龙虎榜有知名游资或机构席位加分
5. 分散题材：不要把所有推荐集中在同一个题材
6. 大盘情绪弱时（评分<5），减少至2只，提高确定性门槛
7. 谨慎对待三板以上高位股（均值回归风险大）

入场条件设定：
- 为每只股票指定具体的入场区间（开盘涨跌幅范围），超出区间则放弃
- 默认入场区间：开盘涨幅在 +1% 到 +5% 之间（高开1%+说明集合竞价有溢价，但低于5%不算过度追高）
- 高确定性标的可放宽上限，弱确定性标的应收窄区间
- 注明T日需要观察的确认信号（如：开盘30分钟内站稳均价线、量能持续放大等）

你必须以严格的JSON格式输出，不要在JSON之外包含任何文本。"""


class OpusDecisionMaker:
    def __init__(self, config: dict):
        self.client = anthropic.Anthropic()
        ai_cfg = config.get("ai", {})
        self.model = ai_cfg.get("opus_model", "claude-opus-4-6-20250515")
        self.max_tokens = ai_cfg.get("opus_max_tokens", 8192)
        self.temperature = ai_cfg.get("temperature", 0.3)

    def decide(self, sonnet_result: dict, market_data: dict) -> dict:
        scored = sonnet_result.get("scored_candidates", [])
        if not scored:
            logger.warning("无评分候选股，跳过 Opus 决策")
            return self._empty_result()

        prompt = self._build_prompt(sonnet_result, market_data)
        logger.info("调用 Opus 进行最终决策...")

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
                    recs = result.get("recommendations", [])
                    logger.info("Opus 决策完成，推荐 %d 只个股", len(recs))
                    return result
                logger.warning("Opus 输出 JSON 解析失败 (尝试 %d/2)", attempt + 1)
            except Exception as e:
                logger.warning("Opus 调用失败 (尝试 %d/2): %s", attempt + 1, e)

        # 降级: 用 Sonnet 结果生成简化推荐
        logger.error("Opus 决策失败，降级使用 Sonnet 结果")
        return self._fallback_from_sonnet(sonnet_result)

    def _build_prompt(self, sonnet_result: dict, market_data: dict) -> str:
        hot_topics = sonnet_result.get("hot_topics", [])
        market_score = sonnet_result.get("market_score", 5)
        sentiment_summary = sonnet_result.get("market_sentiment_summary", "")
        scored = sonnet_result.get("scored_candidates", [])
        sentiment = market_data.get("sentiment", {})

        # 热点题材
        topics_text = "\n".join(
            f"  - {t['topic']}（催化：{t.get('catalyst', 'N/A')}，强度：{t.get('strength', 'N/A')}）\n    {t.get('reasoning', '')}"
            for t in hot_topics
        ) if hot_topics else "  无明确热点"

        # 候选股评分排名
        scored_sorted = sorted(scored, key=lambda x: x.get("score", 0), reverse=True)
        cand_lines = []
        for i, c in enumerate(scored_sorted, 1):
            line = (
                f"  {i}. {c.get('code', '')} {c.get('name', '')} | "
                f"评分:{c.get('score', 'N/A')} | "
                f"匹配题材:{c.get('matched_theme', 'N/A')} | "
                f"分析:{c.get('analysis', 'N/A')} | "
                f"风险:{c.get('risk', 'N/A')}"
            )
            cand_lines.append(line)

        return f"""## 分析师初步结果

### 识别的热点题材
{topics_text}

### 市场情绪评分：{market_score}/10
{sentiment_summary}

### 候选股评分排名
{chr(10).join(cand_lines)}

### 市场数据摘要
- 涨停数/跌停数：{sentiment.get('limit_up_count', 'N/A')}/{sentiment.get('limit_down_count', 'N/A')}
- 炸板率：{sentiment.get('failed_limit_rate', 'N/A')}%
- 北向资金：{market_data.get('northbound', {}).get('northbound_net', 'N/A')}亿

请输出观察名单（4只），JSON格式：
{{
  "market_score": 7,
  "market_assessment": "今日市场情绪评估...",
  "recommendations": [
    {{
      "rank": 1,
      "code": "000001",
      "name": "示例股票",
      "opus_score": 9.0,
      "theme": "题材",
      "reason": "入选观察名单理由（3-5句话，包含量价分析、题材逻辑、资金面验证）",
      "risk_warning": "具体风险提示（2-3点）",
      "entry_strategy": "9:30-10:00观察，开盘涨幅-2%~+3%区间可入场，需确认XXX信号",
      "entry_gap_min": -2.0,
      "entry_gap_max": 3.0,
      "confirm_signal": "开盘30分钟内需要观察到的确认信号（如站稳均价线、量能放大等）",
      "position_pct": 20
    }}
  ],
  "risk_summary": "今日整体风险评估...",
  "position_advice": "建议总仓位X%"
}}"""

    def _parse_json(self, text: str) -> "dict | None":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        match = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        return None

    def _fallback_from_sonnet(self, sonnet_result: dict) -> dict:
        scored = sonnet_result.get("scored_candidates", [])
        scored_sorted = sorted(scored, key=lambda x: x.get("score", 0), reverse=True)
        top = scored_sorted[:4]

        recommendations = []
        for i, c in enumerate(top, 1):
            recommendations.append({
                "rank": i,
                "code": c.get("code", ""),
                "name": c.get("name", ""),
                "opus_score": c.get("score", 0),
                "theme": c.get("matched_theme", ""),
                "reason": c.get("analysis", ""),
                "risk_warning": c.get("risk", ""),
                "entry_strategy": "9:30-10:00观察，开盘涨幅-2%~+3%区间可入场",
                "entry_gap_min": -2.0,
                "entry_gap_max": 3.0,
                "confirm_signal": "需确认开盘30分钟量能不低于昨日同期",
                "position_pct": 15,
            })

        return {
            "market_score": sonnet_result.get("market_score", 5),
            "market_assessment": "【降级模式】Opus 不可用，基于 Sonnet 分析结果生成观察名单",
            "recommendations": recommendations,
            "risk_summary": "降级模式下确定性较低，建议降低仓位，严格执行入场条件",
            "position_advice": "建议总仓位不超过30%",
        }

    def _empty_result(self) -> dict:
        return {
            "market_score": 5,
            "market_assessment": "无足够数据进行分析",
            "recommendations": [],
            "risk_summary": "数据不足，建议观望",
            "position_advice": "建议空仓观望",
        }
