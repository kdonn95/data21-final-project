CREATE VIEW [Weekly Performance] AS
SELECT
    c.candidate_id,
    wp.week_no,
    wp.analytic,
    wp.independent,
    wp.determined,
    wp.professional,
    wp.studious,
    wp.imaginative
FROM candidate c
INNER JOIN weekly_performance wp
    ON wp.candidate_id = c.candidate_id;
