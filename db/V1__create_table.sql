CREATE TABLE IF NOT EXISTS tick_ema_window_data (
    id_index TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    last DOUBLE PRECISION NOT NULL,
    ema38 DOUBLE PRECISION NOT NULL,
    ema100 DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticks_id_time ON tick_ema_window_data (id_index, window_start DESC);
