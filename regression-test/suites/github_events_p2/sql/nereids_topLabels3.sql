SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
-- SELECT
--     sum(bug) AS bugs,
--     sum(feature) AS feature,
--     sum(bug) / sum(feature) AS ratio
-- FROM
-- (
--     SELECT
--         CASE WHEN lower(label) LIKE '%bug%' THEN 1 ELSE 0 END AS bug,
--         CASE WHEN lower(label) LIKE '%feature%' THEN 1 ELSE 0 END AS feature
--     FROM github_events
--     LATERAL VIEW explode_split(labels, ',') t AS label
--     WHERE (event_type IN ('IssuesEvent', 'PullRequestEvent', 'IssueCommentEvent')) AND (action IN ('created', 'opened', 'labeled')) AND ((lower(label) LIKE '%bug%') OR (lower(label) LIKE '%feature%'))
-- ) t
-- LIMIT 50
