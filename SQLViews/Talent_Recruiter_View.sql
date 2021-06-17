CREATE VIEW [Talent Recruiters] AS
SELECT 
    c.candidate_id,
    s.staff_name
FROM candidate c
INNER JOIN staff s
    ON s.staff_id = c.staff_id;
