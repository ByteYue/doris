SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
-- SELECT
--   repo_name,
--   total_stars,
--   round(clickhouse_stars / total_stars, 2) AS ratio
-- FROM
-- (
--     SELECT
--         repo_name,
--         count(distinct actor_login) AS total_stars
--     FROM github_events
--     WHERE (event_type = 'WatchEvent') AND (repo_name NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
--     GROUP BY repo_name
--     HAVING total_stars >= 100
-- ) t1
-- JOIN
-- (
--     SELECT
--         count(distinct actor_login) AS clickhouse_stars
--     FROM github_events
--     WHERE (event_type = 'WatchEvent') AND (repo_name IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
-- ) t2
-- ORDER BY ratio DESC
-- LIMIT 50
