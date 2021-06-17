CREATE VIEW [Candidate Strengths] AS
SELECT
    c.candidate_id,
    s.strength
FROM candidate c
INNER JOIN strength_junc sj
    ON sj.candidate_id = c.candidate_id
INNER JOIN strengths s
    ON s.strength_id = sj.strength_id;
