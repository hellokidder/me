"""管道对抗性评估器 —— 在每个阶段边界做客观验证，零 AI 调用。"""

import logging
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd

logger = logging.getLogger("trading.evaluation")


class Verdict(Enum):
    PASS = "pass"
    RETRY = "retry"
    WARN = "warn"
    HALT = "halt"


@dataclass
class CheckResult:
    name: str
    ok: bool
    message: str
    severity: str  # "critical" / "warning" / "info"


@dataclass
class StageVerdict:
    verdict: Verdict
    stage: str
    checks: list = field(default_factory=list)
    retry_params: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.verdict in (Verdict.PASS, Verdict.WARN)

    @property
    def warnings(self) -> list:
        return [c.message for c in self.checks if not c.ok]


class PipelineEvaluator:
    def __init__(self, config: dict):
        self.cfg = config.get("screening", {})
        self._warnings: list[str] = []

    # ------------------------------------------------------------------
    # 阶段 1: 数据采集
    # ------------------------------------------------------------------
    def evaluate_collection(self, market_data: dict) -> StageVerdict:
        checks = []

        # 指数数据完整性
        indices = market_data.get("indices") or {}
        valid_indices = sum(
            1 for k in ("sh_index_close", "sz_index_close", "cyb_index_close")
            if indices.get(k) is not None
        )
        checks.append(CheckResult(
            "indices_have_values",
            valid_indices >= 2,
            f"指数收盘价仅 {valid_indices}/3 个有效" if valid_indices < 2 else "指数数据完整",
            "critical",
        ))

        # 涨停池非空
        lup = market_data.get("limit_up_pool")
        lup_ok = isinstance(lup, pd.DataFrame) and len(lup) > 0
        checks.append(CheckResult(
            "limit_up_pool_nonempty",
            lup_ok,
            "涨停池为空，可能 API 异常" if not lup_ok else "涨停池数据正常",
            "critical",
        ))

        # 关键数据源计数
        critical_keys = [
            "indices", "limit_up_pool", "strong_pool",
            "dragon_tiger", "northbound", "overnight",
        ]
        source_count = 0
        for key in critical_keys:
            val = market_data.get(key)
            if val is None:
                continue
            if isinstance(val, pd.DataFrame):
                if len(val) > 0:
                    source_count += 1
            elif isinstance(val, dict):
                if any(v is not None for v in val.values()):
                    source_count += 1
            elif isinstance(val, list) and len(val) > 0:
                source_count += 1
        checks.append(CheckResult(
            "critical_source_count",
            source_count >= 4,
            f"关键数据源仅 {source_count}/6 个返回有效数据" if source_count < 4
            else f"关键数据源 {source_count}/6 正常",
            "critical",
        ))

        # 情绪指标一致性
        sentiment = market_data.get("sentiment") or {}
        lup_count = sentiment.get("limit_up_count")
        sentiment_ok = lup_count is not None and (not lup_ok or lup_count > 0)
        checks.append(CheckResult(
            "sentiment_computed",
            sentiment_ok,
            "情绪指标与涨停池数据不一致" if not sentiment_ok else "情绪指标正常",
            "warning",
        ))

        # 新闻数量
        news = market_data.get("news_headlines") or []
        checks.append(CheckResult(
            "news_present",
            len(news) >= 3,
            f"新闻仅 {len(news)} 条，题材分析可能不充分" if len(news) < 3 else "新闻数据正常",
            "warning",
        ))

        return self._build_verdict("collection", checks)

    # ------------------------------------------------------------------
    # 阶段 2: 筛选
    # ------------------------------------------------------------------
    def evaluate_screening(
        self, candidates: list, market_data: dict, attempt: int = 0
    ) -> StageVerdict:
        checks = []

        # 非空
        checks.append(CheckResult(
            "candidates_nonempty",
            len(candidates) > 0,
            "筛选结果为空" if not candidates else f"筛选出 {len(candidates)} 只候选",
            "critical",
        ))

        # 最小数量
        min_count = self.cfg.get("min_candidates", 5)
        checks.append(CheckResult(
            "candidates_min_count",
            len(candidates) >= min_count,
            f"候选 {len(candidates)} 只，低于最小阈值 {min_count}"
            if len(candidates) < min_count else "候选数量达标",
            "warning",
        ))

        # 必要字段
        bad_fields = [
            c.get("code", "?") for c in candidates
            if not c.get("code") or not c.get("name") or not (c.get("close") or 0) > 0
        ]
        checks.append(CheckResult(
            "required_fields",
            len(bad_fields) == 0,
            f"候选字段缺失: {bad_fields[:3]}" if bad_fields else "候选字段完整",
            "critical",
        ))

        # 去重
        codes = [c.get("code") for c in candidates]
        checks.append(CheckResult(
            "no_duplicate_codes",
            len(set(codes)) == len(codes),
            f"存在重复代码" if len(set(codes)) != len(codes) else "无重复代码",
            "warning",
        ))

        # 量比数据覆盖
        if candidates:
            with_vol = sum(1 for c in candidates if (c.get("volume_vs_5d_avg") or 0) > 0)
            ratio = with_vol / len(candidates)
            checks.append(CheckResult(
                "volume_data_present",
                ratio >= 0.5,
                f"仅 {with_vol}/{len(candidates)} 只候选有量比数据"
                if ratio < 0.5 else "量比数据覆盖正常",
                "warning",
            ))

        verdict = self._build_verdict("screening", checks)

        # 反馈循环: 候选不足且首次尝试 → 放宽参数重试
        if not candidates and attempt == 0:
            verdict.verdict = Verdict.RETRY
            verdict.retry_params = {
                "volume_vs_5d_avg_min": self.cfg.get("volume_vs_5d_avg_min", 2.0) * 0.6,
                "turnover_rate_min": self.cfg.get("turnover_rate_min", 3.0) * 0.7,
                "turnover_rate_max": self.cfg.get("turnover_rate_max", 15.0) * 1.3,
                "volume_ratio_min": self.cfg.get("volume_ratio_min", 2.0) * 0.6,
            }
        elif not candidates and attempt > 0:
            # 第二次仍为空，降级为 WARN 继续
            verdict.verdict = Verdict.WARN

        return verdict

    # ------------------------------------------------------------------
    # 阶段 3: Sonnet 分析
    # ------------------------------------------------------------------
    def evaluate_sonnet(self, sonnet_result: dict, candidates: list) -> StageVerdict:
        checks = []
        valid_codes = {c.get("code") for c in candidates}

        # 热点题材
        topics = sonnet_result.get("hot_topics") or []
        checks.append(CheckResult(
            "has_hot_topics",
            len(topics) >= 1,
            "未识别出任何热点题材" if not topics else f"识别 {len(topics)} 个热点题材",
            "warning",
        ))

        # 代码匹配（检测幻觉）
        scored = sonnet_result.get("scored_candidates") or []
        hallucinated = [s.get("code") for s in scored if s.get("code") not in valid_codes]
        codes_ok = len(hallucinated) == 0
        checks.append(CheckResult(
            "scored_candidates_match",
            codes_ok,
            f"Sonnet 幻觉代码: {hallucinated}" if not codes_ok else "候选代码全部匹配",
            "critical",
        ))

        # 分数范围
        bad_scores = [
            s.get("code") for s in scored
            if not isinstance(s.get("score"), (int, float)) or not 1 <= s["score"] <= 10
        ]
        checks.append(CheckResult(
            "scores_in_range",
            len(bad_scores) == 0,
            f"分数异常: {bad_scores[:3]}" if bad_scores else "分数范围正常",
            "critical",
        ))

        # 满分检查
        perfect = [s for s in scored if s.get("score") == 10]
        checks.append(CheckResult(
            "no_perfect_scores",
            len(perfect) <= 1,
            f"{len(perfect)} 只候选满分，区分度不足" if len(perfect) > 1 else "评分区分度正常",
            "warning",
        ))

        # 空结果兜底检查
        is_empty = not scored and sonnet_result.get("market_sentiment_summary") == "分析数据不足"
        checks.append(CheckResult(
            "not_empty_result",
            not is_empty,
            "Sonnet 返回了空结果兜底" if is_empty else "Sonnet 返回有效结果",
            "warning",
        ))

        verdict = self._build_verdict("sonnet", checks)

        # 反馈循环: 幻觉代码 → 直接过滤（零成本修复）
        if hallucinated:
            sonnet_result["scored_candidates"] = [
                s for s in scored if s.get("code") in valid_codes
            ]
            logger.warning("已过滤 Sonnet 幻觉代码: %s", hallucinated)
            verdict.verdict = Verdict.RETRY  # 信号 main.py 重新评估

        return verdict

    # ------------------------------------------------------------------
    # 阶段 4: Opus 决策
    # ------------------------------------------------------------------
    def evaluate_opus(self, opus_result: dict, sonnet_result: dict) -> StageVerdict:
        checks = []
        recs = opus_result.get("recommendations") or []
        sonnet_codes = {
            s.get("code") for s in (sonnet_result.get("scored_candidates") or [])
        }

        # 推荐数量
        market_score = opus_result.get("market_score", 5)
        empty_ok = isinstance(market_score, (int, float)) and market_score < 4
        checks.append(CheckResult(
            "recommendations_present",
            len(recs) > 0 or empty_ok,
            "无推荐且市场评分不低，可能 Opus 异常" if not (len(recs) > 0 or empty_ok)
            else f"推荐 {len(recs)} 只",
            "warning",
        ))

        # 代码匹配（检测幻觉）
        hallucinated = [r.get("code") for r in recs if r.get("code") not in sonnet_codes]
        checks.append(CheckResult(
            "codes_valid",
            len(hallucinated) == 0,
            f"Opus 幻觉代码: {hallucinated}" if hallucinated else "推荐代码全部有效",
            "critical",
        ))

        # 分数范围
        bad_scores = [
            r.get("code") for r in recs
            if not isinstance(r.get("opus_score"), (int, float))
            or not 1 <= r["opus_score"] <= 10
        ]
        checks.append(CheckResult(
            "scores_in_range",
            len(bad_scores) == 0,
            f"Opus 分数异常: {bad_scores[:3]}" if bad_scores else "Opus 分数范围正常",
            "critical",
        ))

        # 仓位合理性
        positions = [r.get("position_pct", 0) for r in recs]
        bad_pos = any(not isinstance(p, (int, float)) or p < 5 or p > 30 for p in positions)
        total = sum(p for p in positions if isinstance(p, (int, float)))
        checks.append(CheckResult(
            "position_sizes_reasonable",
            not bad_pos and total <= 100,
            f"仓位异常: 单只超限或总计 {total}% > 100%" if bad_pos or total > 100
            else f"仓位合理，总计 {total}%",
            "critical",
        ))

        # 题材分散度
        if len(recs) >= 3:
            themes = {r.get("theme", "") for r in recs if r.get("theme")}
            checks.append(CheckResult(
                "not_all_same_theme",
                len(themes) >= 2,
                "推荐集中在单一题材，风险较高" if len(themes) < 2 else f"覆盖 {len(themes)} 个题材",
                "warning",
            ))

        # 降级模式检测
        assessment = opus_result.get("market_assessment", "")
        is_fallback = "降级模式" in assessment
        checks.append(CheckResult(
            "is_not_fallback",
            not is_fallback,
            "当前运行在 Opus 降级模式" if is_fallback else "Opus 正常运行",
            "info",
        ))

        verdict = self._build_verdict("opus", checks)

        # 反馈循环: 幻觉代码过滤 + 仓位钳位
        modified = False
        if hallucinated:
            opus_result["recommendations"] = [
                r for r in recs if r.get("code") in sonnet_codes
            ]
            logger.warning("已过滤 Opus 幻觉代码: %s", hallucinated)
            modified = True

        if bad_pos or total > 100:
            for r in opus_result.get("recommendations", []):
                p = r.get("position_pct", 15)
                if isinstance(p, (int, float)):
                    r["position_pct"] = max(5, min(30, p))
            logger.warning("已钳位异常仓位比例")
            modified = True

        if modified:
            verdict.verdict = Verdict.RETRY

        return verdict

    # ------------------------------------------------------------------
    # 汇总
    # ------------------------------------------------------------------
    def final_summary(self) -> list[str]:
        return list(self._warnings)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _build_verdict(self, stage: str, checks: list[CheckResult]) -> StageVerdict:
        has_critical_fail = any(not c.ok and c.severity == "critical" for c in checks)
        has_warning_fail = any(not c.ok and c.severity in ("warning", "info") for c in checks)

        if has_critical_fail:
            verdict = Verdict.HALT
        elif has_warning_fail:
            verdict = Verdict.WARN
        else:
            verdict = Verdict.PASS

        sv = StageVerdict(verdict=verdict, stage=stage, checks=checks)

        # 记录警告
        for c in checks:
            if not c.ok:
                self._warnings.append(c.message)
                log_fn = logger.warning if c.severity == "critical" else logger.info
                log_fn("[%s] %s: %s", stage, c.name, c.message)
            else:
                logger.debug("[%s] %s: %s", stage, c.name, c.message)

        return sv
