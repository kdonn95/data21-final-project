CREATE VIEW [Recruiter Proportion] AS
SELECT 
    s.staff_id,
    s.staff_name,
    COUNT(1) AS [total_recruited]
FROM staff s
INNER JOIN candidate c
    ON c.staff_id = s.staff_id
INNER JOIN weekly_performance wp
    ON wp.candidate_id = c.candidate_id
GROUP BY s.staff_id, s.staff_name;
