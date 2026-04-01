# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime

logger = logging.getLogger("trading.report")


class ReportGenerator:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate(self, date: str, opus_result: dict, market_data: dict) -> str:
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        market_score = opus_result.get("market_score", "N/A")
        assessment = opus_result.get("market_assessment", "")
        recommendations = opus_result.get("recommendations", [])
        risk_summary = opus_result.get("risk_summary", "")
        position_advice = opus_result.get("position_advice", "")

        indices = market_data.get("indices", {})
        overnight = market_data.get("overnight", {})
        northbound = market_data.get("northbound", {})
        sentiment = market_data.get("sentiment", {})

        lines = [
            f"# {formatted_date} 盘前观察名单",
            "",
            "## 操作流程",
            "",
            "```",
            "1. 盘前: 阅读本报告，了解今日观察标的和入场条件",
            "2. 9:30 开盘: 观察名单中各股的开盘涨跌幅",
            "3. 9:30-10:00: 确认走势（是否站稳、量能是否持续）",
            "4. 符合入场条件 → 买入 | 不符合 → 放弃，不追",
            "5. T+1: 择机卖出",
            "```",
            "",
            f"## 大盘情绪: {market_score}/10",
            "",
            assessment,
            "",
            "### 市场数据",
            "",
            f"- 上证: {indices.get('sh_index_close', 'N/A')}({indices.get('sh_index_change', 'N/A')}%)"
            f" | 深证: {indices.get('sz_index_close', 'N/A')}({indices.get('sz_index_change', 'N/A')}%)"
            f" | 创业板: {indices.get('cyb_index_close', 'N/A')}({indices.get('cyb_index_change', 'N/A')}%)",
            f"- 北向资金: {northbound.get('northbound_net', 'N/A')}亿"
            f" | 涨停: {sentiment.get('limit_up_count', 'N/A')}"
            f" | 跌停: {sentiment.get('limit_down_count', 'N/A')}"
            f" | 炸板率: {sentiment.get('failed_limit_rate', 'N/A')}%",
            f"- 隔夜: 标普{overnight.get('us_sp500_change', 'N/A')}%"
            f" | 纳指{overnight.get('us_nasdaq_change', 'N/A')}%"
            f" | 恒生{overnight.get('hk_hsi_change', 'N/A')}%",
            "",
        ]

        # 热点题材汇总
        themes = {}
        for r in recommendations:
            theme = r.get("theme", "")
            if theme:
                if theme not in themes:
                    themes[theme] = []
                themes[theme].append(f"{r.get('name', '')}({r.get('code', '')})")
        if themes:
            lines.append("## 今日热点题材")
            lines.append("")
            for t, stocks in themes.items():
                lines.append(f"- **{t}**: {', '.join(stocks)}")
            lines.append("")

        # 观察名单
        lines.append("## 观察名单")
        lines.append("")

        if recommendations:
            for r in recommendations:
                rank = r.get("rank", "")
                name = r.get("name", "")
                code = r.get("code", "")
                score = r.get("opus_score", "")
                reason = r.get("reason", "")
                risk = r.get("risk_warning", "")
                entry = r.get("entry_strategy", "")
                confirm = r.get("confirm_signal", "")
                gap_min = r.get("entry_gap_min", -2.0)
                gap_max = r.get("entry_gap_max", 3.0)
                pos = r.get("position_pct", "")

                lines.extend([
                    f"### {rank}. {name}({code}) - 评分 {score}",
                    "",
                    f"**题材**: {r.get('theme', 'N/A')}",
                    "",
                    f"**入选理由**: {reason}",
                    "",
                    f"**入场条件**:",
                    f"- 观察窗口: 9:30-10:00",
                    f"- 入场区间: 开盘涨跌幅在 **{gap_min:+.0f}% ~ {gap_max:+.0f}%** 之间",
                    f"- 确认信号: {confirm or entry}",
                    f"- 超出区间则放弃，不追高",
                    "",
                    f"**仓位**: {pos}% | **风险**: {risk}",
                    "",
                ])
        else:
            lines.extend([
                "今日无符合条件的观察标的，建议空仓观望。",
                "",
            ])

        # 数据质量提示
        eval_warnings = opus_result.get("evaluator_warnings", [])
        if eval_warnings:
            lines.extend(["## 数据质量提示", ""])
            for w in eval_warnings:
                lines.append(f"- {w}")
            lines.append("")

        # 底部
        lines.extend([
            "## 风险提示",
            "",
            risk_summary,
            "",
            f"**仓位建议**: {position_advice}",
            "",
            "---",
            "",
            f"*生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "",
            "*本报告为盘前观察名单，不构成直接买入建议。必须在盘中确认入场条件后才可操作。*",
        ])

        content = "\n".join(lines)
        file_path = os.path.join(self.output_dir, f"{formatted_date}.md")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info("报告已生成: %s", file_path)
        return file_path
