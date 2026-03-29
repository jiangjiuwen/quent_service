import sqlite3
from contextlib import contextmanager
from config.settings import DB_PATH

SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30000


class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    @contextmanager
    def get_connection(self):
        """获取数据库连接（上下文管理器）"""
        conn = sqlite3.connect(self.db_path, timeout=SQLITE_TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple = ()):
        """执行SQL语句"""
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor

    def fetchone(self, sql: str, params: tuple = ()):
        """查询单条记录"""
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None

    def fetchall(self, sql: str, params: tuple = ()):
        """查询多条记录"""
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_columns(self, table_name: str):
        """获取表字段列表"""
        with self.get_connection() as conn:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            return {row["name"] for row in rows}

    def ensure_columns(self, table_name: str, columns: dict):
        """确保表包含所需字段"""
        existing_columns = self.get_columns(table_name)
        with self.get_connection() as conn:
            for column_name, column_definition in columns.items():
                if column_name not in existing_columns:
                    conn.execute(
                        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
                    )

    def migrate_legacy_indices(self) -> int:
        """将历史遗留在股票池中的指数记录迁移到独立指数池"""
        rows = self.fetchall(
            """
            SELECT
                stock_code,
                stock_name,
                market_type,
                COALESCE(exchange, market_type) AS exchange,
                list_date,
                delist_date,
                status,
                created_at,
                updated_at
            FROM stocks
            WHERE source IS NULL
              AND exchange IS NULL
              AND board IS NULL
              AND sec_type IS NULL
              AND (stock_code LIKE '399%%' OR stock_name LIKE '%%指数%%')
            ORDER BY stock_code
            """
        )
        if not rows:
            return 0

        insert_sql = """
            INSERT INTO indices (
                index_code, index_name, market_type, exchange, index_type,
                list_date, delist_date, status, source, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(index_code) DO UPDATE SET
                index_name = excluded.index_name,
                market_type = COALESCE(excluded.market_type, indices.market_type),
                exchange = COALESCE(excluded.exchange, indices.exchange),
                index_type = COALESCE(excluded.index_type, indices.index_type),
                list_date = COALESCE(excluded.list_date, indices.list_date),
                delist_date = COALESCE(excluded.delist_date, indices.delist_date),
                status = COALESCE(excluded.status, indices.status),
                source = COALESCE(excluded.source, indices.source),
                created_at = COALESCE(indices.created_at, excluded.created_at),
                updated_at = COALESCE(excluded.updated_at, indices.updated_at)
        """
        delete_sql = "DELETE FROM stocks WHERE stock_code = ?"
        params = [
            (
                row["stock_code"],
                row["stock_name"],
                row["market_type"],
                row["exchange"],
                "LEGACY_INDEX",
                row["list_date"],
                row["delist_date"],
                row["status"],
                "legacy_stock_migration",
                row["created_at"],
                row["updated_at"],
            )
            for row in rows
        ]

        with self.get_connection() as conn:
            conn.executemany(insert_sql, params)
            conn.executemany(delete_sql, [(row["stock_code"],) for row in rows])
        return len(rows)

    def init_tables(self):
        """初始化数据库表"""
        create_tables_sql = """
        -- 股票基础信息表
        CREATE TABLE IF NOT EXISTS stocks (
            stock_code VARCHAR(10) PRIMARY KEY,
            stock_name VARCHAR(100) NOT NULL,
            market_type VARCHAR(10) NOT NULL,
            exchange VARCHAR(16),
            board VARCHAR(32),
            sec_type VARCHAR(32),
            list_date DATE,
            delist_date DATE,
            status INTEGER DEFAULT 1,
            is_st_current INTEGER DEFAULT 0,
            total_shares DECIMAL(20,4),
            float_shares DECIMAL(20,4),
            industry_code VARCHAR(10),
            source VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 日K线数据表
        CREATE TABLE IF NOT EXISTS daily_kline (
            stock_code VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            open_price DECIMAL(10,4) NOT NULL,
            high_price DECIMAL(10,4) NOT NULL,
            low_price DECIMAL(10,4) NOT NULL,
            close_price DECIMAL(10,4) NOT NULL,
            pre_close DECIMAL(10,4),
            pct_change DECIMAL(10,6),
            volume BIGINT NOT NULL,
            amount DECIMAL(20,4),
            turnover_rate DECIMAL(8,4),
            pe_ratio DECIMAL(10,4),
            pb_ratio DECIMAL(10,4),
            source VARCHAR(32),
            price_mode VARCHAR(16),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, trade_date)
        );

        -- 财务报表数据表
        CREATE TABLE IF NOT EXISTS financial_reports (
            stock_code VARCHAR(10) NOT NULL,
            report_period VARCHAR(10) NOT NULL,
            report_type INTEGER NOT NULL,
            announce_date DATE,
            report_period_end DATE,
            statement_type VARCHAR(32),
            currency VARCHAR(16),
            total_assets DECIMAL(20,4),
            total_liabilities DECIMAL(20,4),
            net_assets DECIMAL(20,4),
            revenue DECIMAL(20,4),
            net_profit DECIMAL(20,4),
            eps DECIMAL(10,4),
            roe DECIMAL(8,4),
            gross_margin DECIMAL(8,4),
            debt_ratio DECIMAL(8,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, report_period)
        );

        -- 行业分类表
        CREATE TABLE IF NOT EXISTS industries (
            industry_code VARCHAR(10) PRIMARY KEY,
            industry_name VARCHAR(100) NOT NULL,
            industry_source VARCHAR(32),
            parent_code VARCHAR(10),
            level INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 交易日历表
        CREATE TABLE IF NOT EXISTS trading_calendar (
            trade_date DATE PRIMARY KEY,
            exchange VARCHAR(16) NOT NULL,
            is_open INTEGER NOT NULL,
            prev_trade_date DATE,
            next_trade_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 指数池
        CREATE TABLE IF NOT EXISTS indices (
            index_code VARCHAR(10) PRIMARY KEY,
            index_name VARCHAR(100) NOT NULL,
            market_type VARCHAR(10),
            exchange VARCHAR(16),
            index_type VARCHAR(32),
            list_date DATE,
            delist_date DATE,
            status INTEGER DEFAULT 1,
            source VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 行业归属历史表
        CREATE TABLE IF NOT EXISTS industry_membership_history (
            stock_code VARCHAR(10) NOT NULL,
            industry_source VARCHAR(32) NOT NULL,
            industry_code VARCHAR(64) NOT NULL,
            industry_name VARCHAR(100) NOT NULL,
            level INTEGER NOT NULL,
            effective_date DATE NOT NULL,
            expire_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, industry_source, effective_date)
        );

        -- 历史股本表
        CREATE TABLE IF NOT EXISTS share_capital_history (
            stock_code VARCHAR(10) NOT NULL,
            effective_date DATE NOT NULL,
            total_shares DECIMAL(20,4),
            float_shares DECIMAL(20,4),
            free_float_shares DECIMAL(20,4),
            source VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, effective_date)
        );

        -- 每日交易状态表
        CREATE TABLE IF NOT EXISTS daily_trade_flags (
            stock_code VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            is_suspended INTEGER DEFAULT 0,
            is_st INTEGER DEFAULT 0,
            is_limit_up INTEGER DEFAULT 0,
            is_limit_down INTEGER DEFAULT 0,
            limit_up_price DECIMAL(10,4),
            limit_down_price DECIMAL(10,4),
            board VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, trade_date)
        );

        -- 复权因子表
        CREATE TABLE IF NOT EXISTS adjust_factors (
            stock_code VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            forward_factor DECIMAL(20,8),
            backward_factor DECIMAL(20,8),
            source VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, trade_date)
        );

        -- 公司行为表
        CREATE TABLE IF NOT EXISTS corporate_actions (
            stock_code VARCHAR(10) NOT NULL,
            ex_date DATE NOT NULL,
            action_type VARCHAR(32) NOT NULL,
            report_year VARCHAR(8),
            cash_dividend_pre_tax DECIMAL(20,6),
            cash_dividend_after_tax DECIMAL(20,6),
            stock_dividend_ratio DECIMAL(20,6),
            reserve_to_stock_ratio DECIMAL(20,6),
            plan_announce_date DATE,
            register_date DATE,
            pay_date DATE,
            source VARCHAR(32),
            raw_plan TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, ex_date, action_type)
        );

        -- 日度估值快照
        CREATE TABLE IF NOT EXISTS daily_valuation_snapshot (
            stock_code VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            market_cap DECIMAL(20,4),
            float_market_cap DECIMAL(20,4),
            pe_ttm DECIMAL(20,6),
            pb_mrq DECIMAL(20,6),
            ps_ttm DECIMAL(20,6),
            pcf_ttm DECIMAL(20,6),
            dividend_yield DECIMAL(10,6),
            source VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, trade_date)
        );

        -- 短线机会评分卡
        CREATE TABLE IF NOT EXISTS stock_factor_scores (
            stock_code VARCHAR(10) PRIMARY KEY,
            stock_name VARCHAR(100) NOT NULL,
            market_type VARCHAR(10) NOT NULL,
            trade_date DATE,
            bars_count INTEGER DEFAULT 0,
            total_score INTEGER NOT NULL DEFAULT 0,
            is_watchlist INTEGER NOT NULL DEFAULT 0,
            trend_pass INTEGER NOT NULL DEFAULT 0,
            momentum_pass INTEGER NOT NULL DEFAULT 0,
            volume_pass INTEGER NOT NULL DEFAULT 0,
            volatility_pass INTEGER NOT NULL DEFAULT 0,
            turnover_pass INTEGER NOT NULL DEFAULT 0,
            close_price DECIMAL(10,4),
            ma20 DECIMAL(10,4),
            ma60 DECIMAL(10,4),
            return_20d DECIMAL(10,6),
            avg_volume_5 DECIMAL(20,4),
            avg_volume_20 DECIMAL(20,4),
            volume_ratio DECIMAL(10,6),
            atr20 DECIMAL(10,6),
            atr20_hist_avg DECIMAL(10,6),
            turnover_rate DECIMAL(10,6),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 市场情绪日度快照
        CREATE TABLE IF NOT EXISTS market_sentiment_daily (
            trade_date DATE PRIMARY KEY,
            sample_size INTEGER NOT NULL DEFAULT 0,
            rising_count INTEGER NOT NULL DEFAULT 0,
            falling_count INTEGER NOT NULL DEFAULT 0,
            flat_count INTEGER NOT NULL DEFAULT 0,
            strong_up_count INTEGER NOT NULL DEFAULT 0,
            strong_down_count INTEGER NOT NULL DEFAULT 0,
            limit_up_count INTEGER NOT NULL DEFAULT 0,
            limit_down_count INTEGER NOT NULL DEFAULT 0,
            failed_limit_count INTEGER NOT NULL DEFAULT 0,
            above_ma20_count INTEGER NOT NULL DEFAULT 0,
            advancing_ratio DECIMAL(10,6),
            above_ma20_ratio DECIMAL(10,6),
            limit_up_ratio DECIMAL(10,6),
            failed_limit_ratio DECIMAL(10,6),
            avg_pct_change DECIMAL(10,6),
            sentiment_score INTEGER NOT NULL DEFAULT 0,
            sentiment_label VARCHAR(32),
            summary VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 行业强度快照
        CREATE TABLE IF NOT EXISTS sector_strength_daily (
            trade_date DATE NOT NULL,
            sector_name VARCHAR(100) NOT NULL,
            stock_count INTEGER NOT NULL DEFAULT 0,
            rising_count INTEGER NOT NULL DEFAULT 0,
            limit_up_count INTEGER NOT NULL DEFAULT 0,
            avg_pct_change DECIMAL(10,6),
            avg_return_5d DECIMAL(10,6),
            above_ma20_ratio DECIMAL(10,6),
            strength_score INTEGER NOT NULL DEFAULT 0,
            leading_stock_code VARCHAR(10),
            leading_stock_name VARCHAR(100),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, sector_name)
        );

        -- 连板 / 炸板等事件信号
        CREATE TABLE IF NOT EXISTS stock_event_signals_daily (
            trade_date DATE NOT NULL,
            stock_code VARCHAR(10) NOT NULL,
            stock_name VARCHAR(100) NOT NULL,
            sector_name VARCHAR(100),
            event_type VARCHAR(32) NOT NULL,
            event_label VARCHAR(32),
            event_value DECIMAL(10,4),
            pct_change DECIMAL(10,6),
            consecutive_days INTEGER NOT NULL DEFAULT 0,
            rank_no INTEGER NOT NULL DEFAULT 0,
            note VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, stock_code, event_type)
        );

        -- 市场资金流快照
        CREATE TABLE IF NOT EXISTS market_fund_flow_daily (
            trade_date DATE PRIMARY KEY,
            sh_close DECIMAL(10,4),
            sh_pct_change DECIMAL(10,6),
            sz_close DECIMAL(10,4),
            sz_pct_change DECIMAL(10,6),
            main_net_inflow DECIMAL(20,4),
            main_net_inflow_ratio DECIMAL(10,6),
            super_large_net_inflow DECIMAL(20,4),
            super_large_net_inflow_ratio DECIMAL(10,6),
            large_net_inflow DECIMAL(20,4),
            large_net_inflow_ratio DECIMAL(10,6),
            mid_net_inflow DECIMAL(20,4),
            mid_net_inflow_ratio DECIMAL(10,6),
            small_net_inflow DECIMAL(20,4),
            small_net_inflow_ratio DECIMAL(10,6),
            source VARCHAR(32),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 板块资金流快照
        CREATE TABLE IF NOT EXISTS sector_fund_flow_daily (
            trade_date DATE NOT NULL,
            sector_type VARCHAR(32) NOT NULL,
            sector_name VARCHAR(100) NOT NULL,
            rank_no INTEGER,
            pct_change DECIMAL(10,6),
            main_net_inflow DECIMAL(20,4),
            main_net_inflow_ratio DECIMAL(10,6),
            super_large_net_inflow DECIMAL(20,4),
            super_large_net_inflow_ratio DECIMAL(10,6),
            large_net_inflow DECIMAL(20,4),
            large_net_inflow_ratio DECIMAL(10,6),
            mid_net_inflow DECIMAL(20,4),
            mid_net_inflow_ratio DECIMAL(10,6),
            small_net_inflow DECIMAL(20,4),
            small_net_inflow_ratio DECIMAL(10,6),
            leading_stock_name VARCHAR(100),
            source VARCHAR(32),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, sector_type, sector_name)
        );

        -- 同步日志表
        CREATE TABLE IF NOT EXISTS sync_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_type VARCHAR(50) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            total_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            error_message TEXT,
            checkpoint_info TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        create_indexes_sql = """
        CREATE INDEX IF NOT EXISTS idx_stocks_market ON stocks(market_type);
        CREATE INDEX IF NOT EXISTS idx_stocks_exchange ON stocks(exchange);
        CREATE INDEX IF NOT EXISTS idx_stocks_board ON stocks(board);
        CREATE INDEX IF NOT EXISTS idx_stocks_industry ON stocks(industry_code);
        CREATE INDEX IF NOT EXISTS idx_stocks_status ON stocks(status);

        CREATE INDEX IF NOT EXISTS idx_indices_market ON indices(market_type);
        CREATE INDEX IF NOT EXISTS idx_indices_exchange ON indices(exchange);
        CREATE INDEX IF NOT EXISTS idx_indices_status ON indices(status);
        CREATE INDEX IF NOT EXISTS idx_indices_source ON indices(source);

        CREATE INDEX IF NOT EXISTS idx_kline_date ON daily_kline(trade_date);
        CREATE INDEX IF NOT EXISTS idx_kline_code_date ON daily_kline(stock_code, trade_date);
        CREATE INDEX IF NOT EXISTS idx_kline_source ON daily_kline(source);

        CREATE INDEX IF NOT EXISTS idx_financial_period ON financial_reports(report_period);

        CREATE INDEX IF NOT EXISTS idx_industries_source ON industries(industry_source);

        CREATE INDEX IF NOT EXISTS idx_calendar_open ON trading_calendar(is_open);

        CREATE INDEX IF NOT EXISTS idx_industry_member_code_date
            ON industry_membership_history(stock_code, effective_date);
        CREATE INDEX IF NOT EXISTS idx_industry_member_industry
            ON industry_membership_history(industry_code);

        CREATE INDEX IF NOT EXISTS idx_share_capital_code_date
            ON share_capital_history(stock_code, effective_date);

        CREATE INDEX IF NOT EXISTS idx_trade_flags_date ON daily_trade_flags(trade_date);
        CREATE INDEX IF NOT EXISTS idx_trade_flags_status
            ON daily_trade_flags(is_suspended, is_st);

        CREATE INDEX IF NOT EXISTS idx_adjust_factors_code_date
            ON adjust_factors(stock_code, trade_date);

        CREATE INDEX IF NOT EXISTS idx_corporate_actions_ex_date
            ON corporate_actions(ex_date);

        CREATE INDEX IF NOT EXISTS idx_daily_valuation_code_date
            ON daily_valuation_snapshot(stock_code, trade_date);

        CREATE INDEX IF NOT EXISTS idx_factor_score_watchlist
            ON stock_factor_scores(is_watchlist, total_score);
        CREATE INDEX IF NOT EXISTS idx_factor_score_trade_date
            ON stock_factor_scores(trade_date);

        CREATE INDEX IF NOT EXISTS idx_market_sentiment_score
            ON market_sentiment_daily(sentiment_score, trade_date);

        CREATE INDEX IF NOT EXISTS idx_sector_strength_rank
            ON sector_strength_daily(trade_date, strength_score DESC);

        CREATE INDEX IF NOT EXISTS idx_stock_event_trade_type
            ON stock_event_signals_daily(trade_date, event_type, rank_no);

        CREATE INDEX IF NOT EXISTS idx_market_fund_flow_trade_date
            ON market_fund_flow_daily(trade_date);

        CREATE INDEX IF NOT EXISTS idx_sector_fund_flow_rank
            ON sector_fund_flow_daily(trade_date, sector_type, rank_no);

        CREATE INDEX IF NOT EXISTS idx_sync_type ON sync_logs(sync_type);
        CREATE INDEX IF NOT EXISTS idx_sync_status ON sync_logs(status);
        CREATE INDEX IF NOT EXISTS idx_sync_time ON sync_logs(created_at);
        """

        with self.get_connection() as conn:
            conn.executescript(create_tables_sql)

        self.ensure_columns(
            "stocks",
            {
                "exchange": "VARCHAR(16)",
                "board": "VARCHAR(32)",
                "sec_type": "VARCHAR(32)",
                "is_st_current": "INTEGER DEFAULT 0",
                "source": "VARCHAR(32)",
            },
        )
        self.ensure_columns(
            "daily_kline",
            {
                "pre_close": "DECIMAL(10,4)",
                "pct_change": "DECIMAL(10,6)",
                "source": "VARCHAR(32)",
                "price_mode": "VARCHAR(16)",
            },
        )
        self.ensure_columns(
            "financial_reports",
            {
                "announce_date": "DATE",
                "report_period_end": "DATE",
                "statement_type": "VARCHAR(32)",
                "currency": "VARCHAR(16)",
            },
        )
        self.ensure_columns(
            "industries",
            {
                "industry_source": "VARCHAR(32)",
            },
        )
        self.ensure_columns(
            "indices",
            {
                "exchange": "VARCHAR(16)",
                "index_type": "VARCHAR(32)",
                "source": "VARCHAR(32)",
                "list_date": "DATE",
                "delist_date": "DATE",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            },
        )
        self.ensure_columns(
            "stock_factor_scores",
            {
                "model_version": "INTEGER NOT NULL DEFAULT 0",
                "tier_label": "VARCHAR(32)",
                "trigger_ready": "INTEGER NOT NULL DEFAULT 0",
                "setup_score": "INTEGER NOT NULL DEFAULT 0",
                "relative_strength_score": "INTEGER NOT NULL DEFAULT 0",
                "breakout_score": "INTEGER NOT NULL DEFAULT 0",
                "volume_score": "INTEGER NOT NULL DEFAULT 0",
                "risk_score": "INTEGER NOT NULL DEFAULT 0",
                "liquidity_score": "INTEGER NOT NULL DEFAULT 0",
                "ma5": "DECIMAL(10,4)",
                "ma10": "DECIMAL(10,4)",
                "return_5d": "DECIMAL(10,6)",
                "return_10d": "DECIMAL(10,6)",
                "benchmark_code": "VARCHAR(10)",
                "benchmark_name": "VARCHAR(100)",
                "benchmark_return_5d": "DECIMAL(10,6)",
                "benchmark_return_10d": "DECIMAL(10,6)",
                "excess_return_5d": "DECIMAL(10,6)",
                "excess_return_10d": "DECIMAL(10,6)",
                "breakout_level": "DECIMAL(10,4)",
                "distance_to_breakout": "DECIMAL(10,6)",
                "latest_volume_ratio": "DECIMAL(10,6)",
                "up_day_volume_ratio_10": "DECIMAL(10,6)",
                "atr5": "DECIMAL(10,6)",
                "atr5_pct": "DECIMAL(10,6)",
                "stop_distance": "DECIMAL(10,6)",
                "avg_amount_20": "DECIMAL(20,4)",
            },
        )

        with self.get_connection() as conn:
            conn.executescript(create_indexes_sql)

        migrated_count = self.migrate_legacy_indices()
        if migrated_count:
            print(f"历史指数迁移完成: {migrated_count} 条")

        print("数据库表初始化完成")


# 全局数据库实例
db = Database()
