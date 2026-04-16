-- DSQL CDC -- Events table
-- Run against your DSQL cluster to create the demo table.
-- Tables MUST have a primary key for CDC to function.

CREATE TABLE IF NOT EXISTS public.events (
    id         UUID DEFAULT gen_random_uuid() NOT NULL,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);
