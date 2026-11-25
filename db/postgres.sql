CREATE TABLE IF NOT EXISTS public.daily_metrics (
    day date NOT NULL,
    metric text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (day, metric, dimension)
);

CREATE TABLE IF NOT EXISTS public.pending_recalculations (
    day date PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS public.daily_exports (
    day date PRIMARY KEY,
    scheduled boolean NOT NULL DEFAULT false,
    revision bigint NOT NULL DEFAULT 1,
    exported_at timestamptz
);
