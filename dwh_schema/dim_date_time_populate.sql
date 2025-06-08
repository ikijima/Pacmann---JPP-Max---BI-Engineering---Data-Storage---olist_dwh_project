WITH date_series AS (
    SELECT generate_series(DATE '2015-01-01', DATE '2020-12-31', INTERVAL '1 day')::DATE AS full_date
),

date_enriched AS (
    SELECT
        gen_random_uuid() AS date_id,
        full_date,
        full_date AS date_only,
        EXTRACT(YEAR FROM full_date)::INT AS year,
        EXTRACT(MONTH FROM full_date)::INT AS month,
        EXTRACT(DAY FROM full_date)::INT AS day,
        EXTRACT(WEEK FROM full_date)::INT AS week,
        EXTRACT(QUARTER FROM full_date)::INT AS quarter,
        CASE WHEN EXTRACT(MONTH FROM full_date) <= 6 THEN 1 ELSE 2 END AS semester,
        TO_CHAR(full_date, 'Day') AS day_of_week,
        TO_CHAR(full_date, 'Month') AS month_name,
        TO_CHAR(full_date, 'YYYY-DDD') AS year_day,
        TO_CHAR(full_date, 'YYYY-MM') AS year_month,
        TO_CHAR(full_date, 'IYYY-IW') AS year_week,
        TO_CHAR(full_date, 'YYYY-"Q"Q') AS year_quarter,
        TO_CHAR(full_date, 'YYYY-"S"') || CASE WHEN EXTRACT(MONTH FROM full_date) <= 6 THEN '1' ELSE '2' END AS year_semester
    FROM date_series
)

INSERT INTO dwh.dim_date (
    date_id, full_date, date_only,
    year, month, day, week, quarter, semester,
    day_of_week, month_name, year_day, year_month, year_week, year_quarter, year_semester
)
SELECT
    date_id, full_date, date_only,
    year, month, day, week, quarter, semester,
    TRIM(day_of_week), TRIM(month_name),
    year_day, year_month, year_week, year_quarter, year_semester
FROM date_enriched
ORDER BY full_date;


DO $$
DECLARE
    h INT;
    m INT;
    s INT;
    time_val TIME;
BEGIN
    FOR h IN 0..23 LOOP
        FOR m IN 0..59 LOOP
            FOR s IN 0..59 LOOP
                time_val := MAKE_TIME(h, m, s);
                INSERT INTO dim_time (time_id, hour, minute, second)
                VALUES (time_val, h, m, s);
            END LOOP;
        END LOOP;
    END LOOP;
END $$;