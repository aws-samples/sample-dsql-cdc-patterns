-- ============================================================================
-- Schema for the DSQL Materialized Table pattern
--
-- Creates the source table (orders), the materialized aggregate
-- (daily_order_totals), and the consumer's bookkeeping table
-- (order_contributions). CDC streams every change to orders to Kinesis,
-- where a Lambda consumer incrementally maintains daily_order_totals.
--
-- DSQL allows one DDL statement per transaction, so run these statements
-- one at a time (deploy.sh and demo.py do this for you).
-- Tables MUST have a primary key for CDC to function.
-- ============================================================================

-- Source table: orders
-- Every INSERT, UPDATE, and DELETE on this table generates a CDC event.
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    total_amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Target: materialized aggregate table
-- Maintained incrementally by the Lambda CDC consumer. Dashboard queries
-- read from this table instead of running GROUP BY on orders.
CREATE TABLE IF NOT EXISTS daily_order_totals (
    order_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14,2) NOT NULL DEFAULT 0.00,
    avg_order_value NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Consumer bookkeeping: what each order currently contributes to
-- daily_order_totals, and the commit timestamp (source.ts_ns) of the CDC
-- event that set it. CDC delivery is at-least-once and unordered, and
-- UPDATE/DELETE events carry no "before" amounts, so the consumer needs this
-- to compute deltas and to ignore duplicate or stale events. Deleted orders
-- are kept as tombstones (is_deleted = true) so a late, older event cannot
-- bring them back.
CREATE TABLE IF NOT EXISTS order_contributions (
    order_id UUID PRIMARY KEY,
    order_date DATE,
    total_amount NUMERIC(12,2),
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    source_ts_ns BIGINT NOT NULL,
    applied_at TIMESTAMP DEFAULT NOW()
);
