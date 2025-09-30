-- init_db.sql
-- PostgreSQL initialization script

-- Create database if not exists
-- CREATE DATABASE warehouse;

-- Transactions summary table
CREATE TABLE IF NOT EXISTS transactions_summary (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    total_transactions BIGINT DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_transaction_value DECIMAL(10, 2) DEFAULT 0,
    unique_customers BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, store_id)
);

-- Real-time metrics table
CREATE TABLE IF NOT EXISTS realtime_metrics (
    id SERIAL PRIMARY KEY,
    metric_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10, 2) NOT NULL,
    dimension VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product performance table
CREATE TABLE IF NOT EXISTS product_performance (
    window_start TIMESTAMP NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    quantity_sold BIGINT DEFAULT 0,
    revenue DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, product_id)
);

-- Raw transactions table (optional - for detailed analysis)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    store_id VARCHAR(50),
    customer_id VARCHAR(50),
    customer_type VARCHAR(20),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(20),
    discount_applied DECIMAL(10, 2),
    final_amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_transactions_summary_window ON transactions_summary(window_start);
CREATE INDEX IF NOT EXISTS idx_transactions_summary_store ON transactions_summary(store_id);
CREATE INDEX IF NOT EXISTS idx_product_performance_window ON product_performance(window_start);
CREATE INDEX IF NOT EXISTS idx_product_performance_product ON product_performance(product_id);
CREATE INDEX IF NOT EXISTS idx_realtime_metrics_time ON realtime_metrics(metric_time);

-- Insert some sample data for testing
INSERT INTO transactions_summary (window_start, window_end, store_id, total_transactions, total_revenue, avg_transaction_value, unique_customers)
VALUES 
    (NOW() - INTERVAL '1 hour', NOW() - INTERVAL '55 minutes', 'London-Central', 45, 2345.67, 52.13, 38),
    (NOW() - INTERVAL '1 hour', NOW() - INTERVAL '55 minutes', 'Manchester-North', 32, 1876.54, 58.64, 28)
ON CONFLICT DO NOTHING;

INSERT INTO product_performance (window_start, product_id, product_name, category, quantity_sold, revenue)
VALUES 
    (NOW() - INTERVAL '1 hour', 'P001', 'Organic Milk', 'Dairy', 125, 311.25),
    (NOW() - INTERVAL '1 hour', 'P002', 'Whole Wheat Bread', 'Bakery', 89, 168.31)
ON CONFLICT DO NOTHING;