CREATE VIEW [Candidate Tech] AS
SELECT
    c.candidate_id,
    t.tech,
    tj.score
FROM candidate c
INNER JOIN tech_junc tj
    ON tj.candidate_id = c.candidate_id
INNER JOIN tech t
    ON t.tech_id = tj.tech_id;
