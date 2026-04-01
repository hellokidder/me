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
            f"# {formatted_date} 今日操作建议",
            "",
            f"## 大盘情绪评分：{market_score} / 10",
            "",
            assessment,
            "",
            "### 市场概况",
            "",
            f"- 上证指数：{indices.get('sh_index_close', 'N/A')}（{indices.get('sh_index_change', 'N/A')}%）",
            f"- 深证成指：{indices.get('sz_index_close', 'N/A')}（{indices.get('sz_index_change', 'N/A')}%）",
            f"- 创业板指：{indices.get('cyb_index_close', 'N/A')}（{indices.get('cyb_index_change', 'N/A')}%）",
            f"- 北向资金：{northbound.get('northbound_net', 'N/A')}亿",
            f"- 涨停数：{sentiment.get('limit_up_count', 'N/A')} | 跌停数：{sentiment.get('limit_down_count', 'N/A')} | 炸板率：{sentiment.get('failed_limit_rate', 'N/A')}%",
            f"- 隔夜标普500：{overnight.get('us_sp500_change', 'N/A')}% | 纳指：{overnight.get('us_nasdaq_change', 'N/A')}% | 恒生：{overnight.get('hk_hsi_change', 'N/A')}%",
            "",
        ]

        # 今日热点题材 (从 market_data 中的 sonnet 结果提取，如果有的话)
        # 热点题材会在 opus_result 中体现，从推荐股的 theme 字段汇总
        themes = set()
        for r in recommendations:
            theme = r.get("theme", "")
            if theme:
                themes.add(theme)
        if themes:
            lines.append("## 今日热点题材")
            lines.append("")
            for t in themes:
                lines.append(f"- {t}")
            lines.append("")

        # 推荐个股
        lines.append("## 推荐个股")
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
                pos = r.get("position_pct", "")

                lines.extend([
                    f"### {rank}. {name}（{code}）",
                    "",
                    f"- **评分**：{score}",
                    f"- **所属题材**：{r.get('theme', 'N/A')}",
                    f"- **推荐理由**：{reason}",
                    f"- **介入策略**：{entry}",
                    f"- **建议仓位**：{pos}%",
                    f"- **风险提示**：{risk}",
                    "",
                ])
        else:
            lines.extend([
                "今日无符合条件的推荐个股，建议观望。",
                "",
            ])

        # 数据质量提示（来自评估器）
        eval_warnings = opus_result.get("evaluator_warnings", [])
        if eval_warnings:
            lines.extend([
                "## 数据质量提示",
                "",
            ])
            for w in eval_warnings:
                lines.append(f"- {w}")
            lines.append("")

        # 风险提示
        lines.extend([
            "## 风险提示",
            "",
            risk_summary,
            "",
            f"**仓位建议**：{position_advice}",
            "",
            "---",
            "",
            f"*报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "",
            "*本报告仅作辅助分析，不构成投资建议。最终决策请自行判断。*",
        ])

        content = "\n".join(lines)

        # 保存文件
        file_path = os.path.join(self.output_dir, f"{formatted_date}.md")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info("报告已生成: %s", file_path)
        return file_path
