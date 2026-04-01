"""回测编排器：批量运行历史日期的完整管道。"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime
from enum import Enum

from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from data.collector import MarketDataCollector
from filter.screener import StockScreener
from analysis.sonnet_analyzer import SonnetAnalyzer
from analysis.opus_decision import OpusDecisionMaker
from evaluation.evaluator import PipelineEvaluator, Verdict
from verification.verifier import Verifier
from backtest.calendar import TradingCalendar

logger = logging.getLogger("trading.backtest")


class BacktestMode(Enum):
    FULL = "full"
    SONNET = "sonnet"
    RULES = "rules"


class CachedSonnetAnalyzer(SonnetAnalyzer):
    """带 prompt 缓存的 Sonnet 分析器。"""

    def __init__(self, config: dict, db: TradingDB):
        super().__init__(config)
        self.db = db

    def analyze(self, market_data: dict, candidates: list) -> dict:
        if not candidates:
            return self._empty_result()

        prompt = self._build_prompt(market_data, candidates)
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
        date = market_data.get("date", "")

        cached = self.db.get_ai_cache(date, "sonnet", prompt_hash)
        if cached:
            logger.info("Sonnet cache hit for %s", date)
            return json.loads(cached["response_json"])

        result = super().analyze(market_data, candidates)
        self.db.save_ai_cache(
            date, "sonnet", prompt_hash, self.model, json.dumps(result, ensure_ascii=False)
        )
        return result


class CachedOpusDecisionMaker(OpusDecisionMaker):
    """带 prompt 缓存的 Opus 决策器。"""

    def __init__(self, config: dict, db: TradingDB):
        super().__init__(config)
        self.db = db

    def decide(self, sonnet_result: dict, market_data: dict) -> dict:
        scored = sonnet_result.get("scored_candidates", [])
        if not scored:
            return self._fallback_from_sonnet(sonnet_result)

        prompt = self._build_prompt(sonnet_result, market_data)
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
        date = market_data.get("date", "")

        cached = self.db.get_ai_cache(date, "opus", prompt_hash)
        if cached:
            logger.info("Opus cache hit for %s", date)
            return json.loads(cached["response_json"])

        result = super().decide(sonnet_result, market_data)
        self.db.save_ai_cache(
            date, "opus", prompt_hash, self.model, json.dumps(result, ensure_ascii=False)
        )
        return result


class BacktestRunner:
    def __init__(self, config: dict, mode: BacktestMode = BacktestMode.SONNET):
        self.config = config
        self.mode = mode
        self.calendar = TradingCalendar()
        paths = config.get("paths", {})
        self.db = TradingDB(paths.get("db_path", "storage/trading.db"))
        self.verifier = Verifier(self.db, config)
        self.rate_limit = config.get("backtest", {}).get("rate_limit_seconds", 2)

    def run(self, start_date: str, end_date: str,
            skip_existing: bool = True) -> dict:
        trading_days = self.calendar.get_trading_days(start_date, end_date)
        logger.info(
            "回测开始: %s ~ %s, 共 %d 个交易日, 模式=%s",
            start_date, end_date, len(trading_days), self.mode.value,
        )

        success = 0
        skipped = 0
        failed = 0

        for i, date in enumerate(trading_days):
            if skip_existing and self.db.has_recommendations(date, "backtest"):
                logger.info("[%d/%d] %s 已有回测结果，跳过", i + 1, len(trading_days), date)
                skipped += 1
                continue

            logger.info("[%d/%d] 回测 %s ...", i + 1, len(trading_days), date)
            try:
                self._run_single_day(date)
                success += 1
            except Exception as e:
                logger.error("回测 %s 失败: %s", date, e)
                failed += 1

            if i < len(trading_days) - 1:
                time.sleep(self.rate_limit)

        logger.info(
            "回测数据生成完成: 成功 %d, 跳过 %d, 失败 %d",
            success, skipped, failed,
        )

        # 批量验证
        logger.info("开始批量验证...")
        stats = self.verifier.verify_batch(start_date, end_date, source="backtest")

        # 生成报告
        report_path = self._generate_report(start_date, end_date, stats)
        logger.info("回测报告: %s", report_path)

        self.db.close()
        return stats

    def _run_single_day(self, date: str):
        """运行单日管道（回测模式）。"""
        evaluator = PipelineEvaluator(self.config)

        # 1. 数据采集（传入 cache_db 启用日线缓存）
        collector = MarketDataCollector(self.config, mode="backtest", cache_db=self.db)
        market_data = collector.collect_all(date)

        v = evaluator.evaluate_collection(market_data)
        if v.verdict == Verdict.HALT:
            # 回测模式下降级为 WARN：历史数据源可用性有限
            logger.warning("%s 数据质量警告（回测降级）: %s", date, v.warnings)

        # 2. 筛选
        screener = StockScreener(self.config, collector)
        candidates = screener.screen(market_data)

        v = evaluator.evaluate_screening(candidates, market_data, attempt=0)
        if v.verdict == Verdict.RETRY:
            retry_config = {
                **self.config,
                "screening": {**self.config.get("screening", {}), **v.retry_params},
            }
            screener = StockScreener(retry_config, collector)
            candidates = screener.screen(market_data)

        self.db.save_candidates(date, candidates, source="backtest")

        if not candidates:
            # 保存空推荐以标记此日已处理
            self.db.save_recommendations(date, [], source="backtest")
            return

        if self.mode == BacktestMode.RULES:
            self._store_rules_recs(date, candidates)
            return

        # 3. Sonnet 分析（带缓存）
        sonnet = CachedSonnetAnalyzer(self.config, self.db)
        sonnet_result = sonnet.analyze(market_data, candidates)

        v = evaluator.evaluate_sonnet(sonnet_result, candidates)
        if v.verdict == Verdict.RETRY:
            evaluator.evaluate_sonnet(sonnet_result, candidates)

        # 回写评分
        scored_map = {c["code"]: c for c in sonnet_result.get("scored_candidates", [])}
        for cand in candidates:
            scored = scored_map.get(cand["code"])
            if scored:
                cand["sonnet_score"] = scored.get("score")
                cand["sonnet_theme"] = scored.get("matched_theme")
        self.db.save_candidates(date, candidates, source="backtest")

        if self.mode == BacktestMode.SONNET:
            self._store_sonnet_recs(date, sonnet_result, candidates)
            return

        # 4. Opus 决策（带缓存）
        opus = CachedOpusDecisionMaker(self.config, self.db)
        opus_result = opus.decide(sonnet_result, market_data)

        v = evaluator.evaluate_opus(opus_result, sonnet_result)
        if v.verdict == Verdict.RETRY:
            evaluator.evaluate_opus(opus_result, sonnet_result)

        self.db.save_recommendations(
            date, opus_result.get("recommendations", []), source="backtest"
        )

    def _store_rules_recs(self, date: str, candidates: list):
        """Rules 模式：取 top 6 候选作为推荐。"""
        recs = []
        for i, c in enumerate(candidates[:6], 1):
            recs.append({
                "rank": i,
                "code": c["code"],
                "name": c["name"],
                "opus_score": c.get("volume_vs_5d_avg", 0),
                "theme": c.get("industry", ""),
                "reason": "规则筛选",
                "risk_warning": "",
                "entry_strategy": "观察开盘",
                "position_pct": 15,
            })
        self.db.save_recommendations(date, recs, source="backtest")

    def _store_sonnet_recs(self, date: str, sonnet_result: dict,
                           candidates: list | None = None):
        """Sonnet 模式：复合评分 top 4 作为推荐。

        公式 v3: ai_score*0.6 + boards*1.5 + vol*0.2 - 高涨幅惩罚
        经 4 轮回测迭代验证：胜率 39.2%，高分组胜率 48.3%。
        """
        scored = sonnet_result.get("scored_candidates", [])
        # 过滤科创板/北交所
        scored = [s for s in scored
                  if not s.get("code", "").startswith(("688", "689", "8", "920"))]
        # 用 screener candidates 的量价数据丰富 Sonnet 结果
        cand_map = {c["code"]: c for c in (candidates or [])}
        for s in scored:
            cand = cand_map.get(s.get("code"), {})
            s["consecutive_boards"] = cand.get("consecutive_boards", 0)
            s["volume_ratio"] = cand.get("volume_ratio", 0)
            s["volume_vs_5d_avg"] = cand.get("volume_vs_5d_avg", 0)
            s["change_pct"] = cand.get("change_pct", 0)
        # 复合排序 v3：AI为主 + 连板加成 + 高涨幅惩罚
        def _composite(c):
            ai_score = c.get("score", 0)
            boards = c.get("consecutive_boards", 0)
            vol_5d = c.get("volume_vs_5d_avg", 0) or 0
            change = abs(c.get("change_pct", 0) or 0)
            penalty = 2.0 if change > 7 else 0
            return ai_score * 0.6 + boards * 1.5 + vol_5d * 0.2 - penalty
        scored_sorted = sorted(scored, key=_composite, reverse=True)
        recs = []
        for i, c in enumerate(scored_sorted[:4], 1):
            recs.append({
                "rank": i,
                "code": c.get("code", ""),
                "name": c.get("name", ""),
                "opus_score": c.get("score", 0),
                "theme": c.get("matched_theme", ""),
                "reason": c.get("analysis", ""),
                "risk_warning": c.get("risk", ""),
                "entry_strategy": "观察开盘，涨超5%放弃",
                "position_pct": 20,
            })
        self.db.save_recommendations(date, recs, source="backtest")

    def _generate_report(self, start: str, end: str, stats: dict) -> str:
        """生成回测 Markdown 报告。"""
        output_dir = self.config.get("paths", {}).get("output_dir", "output")
        os.makedirs(output_dir, exist_ok=True)

        s_fmt = f"{start[:4]}-{start[4:6]}-{start[6:]}"
        e_fmt = f"{end[:4]}-{end[4:6]}-{end[6:]}"

        lines = [
            f"# 回测报告: {s_fmt} ~ {e_fmt}",
            "",
            f"**模式**: {self.mode.value}",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 总体统计",
            "",
            f"| 指标 | 值 |",
            f"|------|-----|",
            f"| 总推荐数 | {stats.get('total_recs', 0)} |",
            f"| 胜率 | {stats.get('win_rate', 0):.1f}% |",
            f"| 平均收益 | {stats.get('avg_close_return', 0):.2f}% |",
            f"| 平均最大收益 | {stats.get('avg_max_return', 0):.2f}% |",
            f"| 最大单笔亏损 | {stats.get('max_single_loss', 0):.2f}% |",
            f"| 最大单笔盈利 | {stats.get('max_single_gain', 0):.2f}% |",
            f"| 可入场率 | {stats.get('entry_feasible_rate', 0):.1f}% |",
            f"| Sharpe-like | {stats.get('sharpe_like', 0):.4f} |",
            "",
        ]

        # 逐日明细
        vr = self.db.get_verification_results(start, end, source="backtest")
        if vr:
            lines.extend([
                "## 逐日明细",
                "",
                "| 日期 | 代码 | 名称 | 评分 | T+1收益 | 最大收益 | 胜负 |",
                "|------|------|------|------|---------|---------|------|",
            ])
            for r in vr:
                win_mark = "W" if r["win"] else "L"
                lines.append(
                    f"| {r['rec_date']} | {r['code']} | {r['name']} "
                    f"| {r.get('opus_score', '-')} "
                    f"| {r['close_return_pct']:.1f}% "
                    f"| {r['max_return_pct']:.1f}% "
                    f"| {win_mark} |"
                )
            lines.append("")

        lines.extend([
            "---",
            "",
            "*本报告由回测系统自动生成，不构成投资建议。*",
        ])

        content = "\n".join(lines)
        file_path = os.path.join(output_dir, f"backtest_{start}_{end}.md")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        return file_path
