import argparse
import os
import sys
from datetime import datetime, timedelta

from utils.config_loader import load_config
from utils.logger import setup_logger
from storage.db import TradingDB
from data.collector import MarketDataCollector
from filter.screener import StockScreener
from analysis.sonnet_analyzer import SonnetAnalyzer
from analysis.opus_decision import OpusDecisionMaker
from report.generator import ReportGenerator
from evaluation.evaluator import PipelineEvaluator, Verdict


def get_last_trading_date() -> str:
    today = datetime.now()
    # 如果是周末，回退到周五
    weekday = today.weekday()
    if weekday == 5:  # 周六
        today -= timedelta(days=1)
    elif weekday == 6:  # 周日
        today -= timedelta(days=2)
    else:
        # 工作日: 用昨天的数据（盘前分析基于昨日数据）
        today -= timedelta(days=1)
        if today.weekday() == 5:
            today -= timedelta(days=1)
        elif today.weekday() == 6:
            today -= timedelta(days=2)
    return today.strftime("%Y%m%d")


def main():
    parser = argparse.ArgumentParser(description="A股超短线辅助分析系统")
    parser.add_argument("--date", type=str, help="目标日期 (YYYYMMDD)，默认取最近交易日")
    args = parser.parse_args()

    # 切换工作目录到项目根目录
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # 加载配置
    config = load_config()
    paths = config.get("paths", {})

    # 初始化日志
    logger = setup_logger(paths.get("log_dir", "logs"))
    logger.info("=" * 50)
    logger.info("A股超短线辅助分析系统 启动")
    logger.info("=" * 50)

    # 确定目标日期
    date = args.date or get_last_trading_date()
    logger.info("分析目标日期: %s", date)

    # 初始化数据库
    db = TradingDB(paths.get("db_path", "storage/trading.db"))

    evaluator = PipelineEvaluator(config)

    try:
        # ===== 1. 数据收集 =====
        collector = MarketDataCollector(config, cache_db=db)
        market_data = collector.collect_all(date)

        v = evaluator.evaluate_collection(market_data)
        if v.verdict == Verdict.HALT:
            logger.error("数据采集质量不达标: %s", v.warnings)
            print(f"\n❌ 数据采集质量不达标，终止分析: {'; '.join(v.warnings)}")
            sys.exit(1)

        # ===== 2. 规则初筛 =====
        screener = StockScreener(config, collector)
        candidates = screener.screen(market_data)

        v = evaluator.evaluate_screening(candidates, market_data, attempt=0)
        if v.verdict == Verdict.RETRY:
            logger.info("候选不足，放宽参数重试: %s", v.retry_params)
            retry_config = {**config, "screening": {**config.get("screening", {}), **v.retry_params}}
            screener = StockScreener(retry_config, collector)
            candidates = screener.screen(market_data)
            v = evaluator.evaluate_screening(candidates, market_data, attempt=1)

        if not candidates:
            logger.warning("初筛无候选股，生成观望报告")

        # 保存候选股到数据库
        db.save_candidates(date, candidates)

        # ===== 3. Sonnet 分析 =====
        sonnet = SonnetAnalyzer(config)
        sonnet_result = sonnet.analyze(market_data, candidates)

        v = evaluator.evaluate_sonnet(sonnet_result, candidates)
        if v.verdict == Verdict.RETRY:
            # 幻觉代码已在 evaluate_sonnet 中被过滤，重新评估确认
            v = evaluator.evaluate_sonnet(sonnet_result, candidates)

        # 将 Sonnet 评分回写候选股
        scored_map = {
            c.get("code"): c
            for c in sonnet_result.get("scored_candidates", [])
        }
        for cand in candidates:
            scored = scored_map.get(cand["code"])
            if scored:
                cand["sonnet_score"] = scored.get("score")
                cand["sonnet_theme"] = scored.get("matched_theme")
        # 更新数据库中的候选股评分
        db.save_candidates(date, candidates)

        # ===== 4. Opus 决策 =====
        opus = OpusDecisionMaker(config)
        opus_result = opus.decide(sonnet_result, market_data)

        v = evaluator.evaluate_opus(opus_result, sonnet_result)
        if v.verdict == Verdict.RETRY:
            # 幻觉代码/仓位已修复，重新评估确认
            v = evaluator.evaluate_opus(opus_result, sonnet_result)

        # 保存推荐到数据库
        db.save_recommendations(date, opus_result.get("recommendations", []))

        # ===== 5. 生成报告 =====
        eval_warnings = evaluator.final_summary()
        if eval_warnings:
            opus_result["evaluator_warnings"] = eval_warnings

        report_gen = ReportGenerator(paths.get("output_dir", "output"))
        report_path = report_gen.generate(date, opus_result, market_data)

        # 保存大盘数据
        indices = market_data.get("indices", {})
        overnight = market_data.get("overnight", {})
        northbound = market_data.get("northbound", {})
        margin = market_data.get("margin", {})
        sentiment = market_data.get("sentiment", {})

        db.save_daily_market({
            "date": f"{date[:4]}-{date[4:6]}-{date[6:]}",
            "sh_index_close": indices.get("sh_index_close"),
            "sh_index_change": indices.get("sh_index_change"),
            "sz_index_close": indices.get("sz_index_close"),
            "sz_index_change": indices.get("sz_index_change"),
            "cyb_index_close": indices.get("cyb_index_close"),
            "cyb_index_change": indices.get("cyb_index_change"),
            "northbound_net": northbound.get("northbound_net"),
            "margin_balance": margin.get("margin_balance"),
            "margin_change": margin.get("margin_change"),
            "limit_up_count": sentiment.get("limit_up_count"),
            "limit_down_count": sentiment.get("limit_down_count"),
            "failed_limit_rate": sentiment.get("failed_limit_rate"),
            "us_sp500_change": overnight.get("us_sp500_change"),
            "us_nasdaq_change": overnight.get("us_nasdaq_change"),
            "hk_hsi_change": overnight.get("hk_hsi_change"),
        })

        logger.info("=" * 50)
        logger.info("分析完成！报告路径: %s", report_path)
        logger.info("推荐个股: %d 只", len(opus_result.get("recommendations", [])))
        logger.info("=" * 50)

        print(f"\n✅ 分析完成！报告已保存至: {report_path}")
        recs = opus_result.get("recommendations", [])
        if recs:
            print(f"\n📋 今日推荐 {len(recs)} 只个股:")
            for r in recs:
                print(f"   {r.get('rank', '')}. {r.get('name', '')}({r.get('code', '')}) - {r.get('theme', '')}")

    except Exception as e:
        logger.error("系统运行异常: %s", e, exc_info=True)
        print(f"\n❌ 运行出错: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
