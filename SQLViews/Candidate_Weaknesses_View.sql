CREATE VIEW [Candidate Weaknesses] AS
SELECT
    c.candidate_id,
    w.weakness
FROM candidate c
INNER JOIN weaknesses_junc wj
    ON wj.candidate_id = c.candidate_id
INNER JOIN weaknesses w
    ON w.weakness_id = wj.weakness_id;
