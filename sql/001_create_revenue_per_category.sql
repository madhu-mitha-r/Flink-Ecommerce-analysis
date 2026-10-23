CREATE TABLE IF NOT EXISTS revenue_per_category (
    window_start  TIMESTAMPTZ    NOT NULL,
    window_end    TIMESTAMPTZ    NOT NULL,
    category      TEXT           NOT NULL,
    units_sold    BIGINT         NOT NULL,
    revenue       NUMERIC(12,2)  NOT NULL,
    PRIMARY KEY (window_start, category)
);
