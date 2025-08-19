-- Analytics Result Tables for SQLite
-- These tables store the results of analytics runs for historical tracking

-- Analytics run metadata
CREATE TABLE IF NOT EXISTS analytics_runs (
    run_id TEXT PRIMARY KEY,
    chat_id TEXT NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_type TEXT NOT NULL, -- 'full', 'incremental', 'scheduled'
    status TEXT NOT NULL, -- 'running', 'completed', 'failed'
    total_messages_analyzed INTEGER,
    total_users_analyzed INTEGER,
    duration_seconds REAL,
    error_message TEXT,
    metadata TEXT -- JSON string with additional info
);

-- Message analytics results
CREATE TABLE IF NOT EXISTS message_analytics_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL,
    metric_details TEXT, -- JSON string with detailed breakdown
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES analytics_runs(run_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
);

-- User analytics results  
CREATE TABLE IF NOT EXISTS user_analytics_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL,
    metric_details TEXT, -- JSON string
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES analytics_runs(run_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Chat analytics results
CREATE TABLE IF NOT EXISTS chat_analytics_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL,
    metric_details TEXT, -- JSON string
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES analytics_runs(run_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
);

-- Aggregated daily statistics (for trending)
CREATE TABLE IF NOT EXISTS daily_chat_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL,
    date DATE NOT NULL,
    total_messages INTEGER,
    active_users INTEGER,
    new_users INTEGER,
    avg_message_length REAL,
    peak_hour INTEGER,
    peak_hour_messages INTEGER,
    questions_count INTEGER,
    replies_count INTEGER,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chat_id, date),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
);

-- User engagement tracking
CREATE TABLE IF NOT EXISTS user_engagement_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    date DATE NOT NULL,
    messages_sent INTEGER,
    questions_asked INTEGER,
    replies_sent INTEGER,
    avg_message_length REAL,
    first_message_time TIME,
    last_message_time TIME,
    active_hours INTEGER, -- Number of distinct hours with activity
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chat_id, user_id, date),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_analytics_runs_chat ON analytics_runs(chat_id);
CREATE INDEX IF NOT EXISTS idx_analytics_runs_timestamp ON analytics_runs(run_timestamp);
CREATE INDEX IF NOT EXISTS idx_message_analytics_run ON message_analytics_results(run_id);
CREATE INDEX IF NOT EXISTS idx_user_analytics_run ON user_analytics_results(run_id);
CREATE INDEX IF NOT EXISTS idx_user_analytics_user ON user_analytics_results(user_id);
CREATE INDEX IF NOT EXISTS idx_chat_analytics_run ON chat_analytics_results(run_id);
CREATE INDEX IF NOT EXISTS idx_daily_stats_chat_date ON daily_chat_stats(chat_id, date);
CREATE INDEX IF NOT EXISTS idx_engagement_user_date ON user_engagement_history(chat_id, user_id, date);