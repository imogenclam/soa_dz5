CREATE USER IF NOT EXISTS cinema_reader IDENTIFIED WITH sha256_password BY 'cinema_read_local';
GRANT SELECT ON cinema.daily_metrics TO cinema_reader;
