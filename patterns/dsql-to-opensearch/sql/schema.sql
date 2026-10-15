-- DSQL CDC — Products table
-- Run against your DSQL cluster to create the demo table.
-- Tables MUST have a primary key for CDC to function.

CREATE TABLE IF NOT EXISTS public.products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(200) NOT NULL,
    description TEXT NOT NULL,
    category    VARCHAR(100) NOT NULL,
    brand       VARCHAR(100) NOT NULL,
    price       DECIMAL(10,2) NOT NULL,
    in_stock    BOOLEAN DEFAULT true,
    tags        TEXT DEFAULT '',
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);
