# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A股超短线盘前辅助分析系统——Python 项目，每日开盘前运行，通过规则筛选 + Claude 两阶段 AI 分析，从涨停板/强势股/龙虎榜中选出 4-6 只短线标的，生成 Markdown 报告。

## Running

```bash
# 安装依赖
pip install -r requirements.txt

# 运行（默认分析上一个交易日）
python main.py

# 分析指定日期
python main.py --date 20260327
```

需要在 `config/.env` 中配置 `ANTHROPIC_API_KEY`。

## Architecture

数据流是单向管道，五个阶段顺序执行：

```
DataCollector → StockScreener → SonnetAnalyzer → OpusDecisionMaker → ReportGenerator
(data/)         (filter/)        (analysis/)       (analysis/)          (report/)
```

- **data/collector.py** — 通过 AkShare 采集 8 个维度的市场数据（指数、涨停池、龙虎榜、北向资金、融资融券、隔夜外盘、新闻、情绪指标）。各数据源适配器在 `data/sources/` 下。
- **filter/screener.py** — 合并涨停/强势/龙虎榜构建股票池，按量比、换手率、技术形态（MA5 金叉、MACD、放量突破）过滤，输出 10-15 只候选。技术指标用纯 pandas 实现，无 TA-Lib 依赖。
- **analysis/sonnet_analyzer.py** — Sonnet 4.5 识别 2-4 个热点题材，对候选股打分(1-10)。要求严格 JSON 输出，失败重试 2 次。
- **analysis/opus_decision.py** — Opus 4.6 从 Sonnet 结果中精选 4-6 只，附带理由、风险提示、入场策略。Opus 失败时降级使用 Sonnet 结果。
- **report/generator.py** — 生成 Markdown 日报到 `output/YYYY-MM-DD.md`。
- **storage/db.py** + **schema.sql** — SQLite 存储每日市场快照、候选股、推荐结果（3 张表：daily_market、candidates、recommendations）。

## Key Design Decisions

- **直接使用 Anthropic SDK**，不用 LangChain/LiteLLM，便于调试和直接控制。
- **Sonnet 负责分析，Opus 负责决策**——成本与推理深度的平衡。
- AI 输出强制 JSON 格式，解析失败有重试和降级机制。
- 纯决策辅助系统，不自动交易。

## Configuration

`config/config.yaml` 包含筛选阈值（量比≥2.0、换手率 3-15%、排除 ST、最高价 100 元等）、AI 模型参数（temperature 0.3）、路径配置。

## Output

- 报告：`output/YYYY-MM-DD.md`
- 日志：`logs/YYYY-MM-DD.log`（文件 DEBUG，控制台 INFO）
- 数据库：`storage/trading.db`
