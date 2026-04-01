CREATE TABLE IF NOT EXISTS daily_market (
    date TEXT PRIMARY KEY,
    sh_index_close REAL,
    sh_index_change REAL,
    sz_index_close REAL,
    sz_index_change REAL,
    cyb_index_close REAL,
    cyb_index_change REAL,
    northbound_net REAL,
    margin_balance REAL,
    margin_change REAL,
    limit_up_count INTEGER,
    limit_down_count INTEGER,
    failed_limit_rate REAL,
    us_sp500_change REAL,
    us_nasdaq_change REAL,
    hk_hsi_change REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    close REAL,
    change_pct REAL,
    volume_ratio REAL,
    turnover_rate REAL,
    volume_vs_5d_avg REAL,
    is_limit_up INTEGER DEFAULT 0,
    consecutive_boards INTEGER DEFAULT 0,
    on_dragon_tiger INTEGER DEFAULT 0,
    industry TEXT,
    sonnet_score REAL,
    sonnet_theme TEXT,
    seal_money REAL,             -- 封板资金(元)
    seal_time TEXT,              -- 首次封板时间(HHMMSS)
    reopen_count INTEGER DEFAULT 0,  -- 炸板次数
    turnover_amount REAL,        -- 成交额(元)
    float_market_cap REAL,       -- 流通市值(元)
    source TEXT DEFAULT 'live',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, code, source)
);

CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    rank INTEGER,
    reason TEXT,
    risk_warning TEXT,
    entry_strategy TEXT,
    opus_score REAL,
    theme TEXT,
    position_pct REAL,
    source TEXT DEFAULT 'live',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, code, source)
);

CREATE TABLE IF NOT EXISTS verification_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rec_date TEXT NOT NULL,       -- T-1: 分析日（推荐基于此日收盘数据）
    buy_date TEXT,                -- T: 买入日
    verify_date TEXT NOT NULL,    -- T+1: 卖出/评估日
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    rec_close REAL,              -- T-1 收盘价
    buy_price REAL,              -- T日 实际买入价（开盘价）
    buy_open REAL,               -- T日 开盘价
    buy_high REAL,               -- T日 最高价
    buy_low REAL,                -- T日 最低价
    buy_close REAL,              -- T日 收盘价
    t1_open REAL,                -- T+1 开盘
    t1_high REAL,                -- T+1 最高
    t1_low REAL,                 -- T+1 最低
    t1_close REAL,               -- T+1 收盘
    entry_gap_pct REAL,          -- 入场缺口: (T日开盘 - T-1收盘) / T-1收盘
    best_return_pct REAL,        -- 最好情况: (T+1最高 - 买入价) / 买入价
    worst_return_pct REAL,       -- 最坏情况: (T+1最低 - 买入价) / 买入价
    close_return_pct REAL,       -- 收盘收益: (T+1收盘 - 买入价) / 买入价
    buy_day_return_pct REAL,     -- T日收益: (T日收盘 - 买入价) / 买入价
    open_return_pct REAL,        -- 兼容旧字段
    max_return_pct REAL,         -- 兼容旧字段
    min_return_pct REAL,         -- 兼容旧字段
    win INTEGER DEFAULT 0,
    entry_feasible INTEGER DEFAULT 0,
    strategy_return_pct REAL,
    opus_score REAL,
    rank INTEGER,
    entry_strategy TEXT,
    source TEXT DEFAULT 'live',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(rec_date, code, source)
);

CREATE TABLE IF NOT EXISTS verification_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    total_recs INTEGER,
    win_count INTEGER,
    loss_count INTEGER,
    win_rate REAL,
    avg_close_return REAL,
    avg_max_return REAL,
    max_single_loss REAL,
    max_single_gain REAL,
    entry_feasible_rate REAL,
    sharpe_like REAL,
    source TEXT DEFAULT 'live',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_daily_cache (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_cache_code_date
    ON stock_daily_cache(code, trade_date);

CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_date TEXT NOT NULL,
    news_time TEXT,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    url TEXT,
    category TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, title, news_date)
);

CREATE INDEX IF NOT EXISTS idx_news_date ON news_items(news_date);

CREATE TABLE IF NOT EXISTS ai_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    stage TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, stage, prompt_hash)
);
