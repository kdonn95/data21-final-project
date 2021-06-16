CREATE VIEW [Candidate Info] AS 
SELECT 
    c.candidate_id,
    c.candidate_name,
    c.email,
    c.address,
    c.city,
    c.postcode,
    co.course_name,
    s.staff_name,
    c.uni_name,
    c.degree_result,
    sd.geo_flex,
    sd.self_development,
    sd.financial_support,
    sd.result,
    ROUND(
        (CAST(sd.presentation AS FLOAT) / CAST(sd.presentation_max AS FLOAT)) 
        * 100, 2
        ) AS presentation_pct,
    ROUND(
        (CAST(sd.psychometrics AS FLOAT) / CAST(sd.psychometrics_max AS FLOAT)) 
        * 100, 2
        ) AS psychometrics_pct
FROM candidate c
INNER JOIN sparta_day sd
    ON sd.candidate_id = c.candidate_id
LEFT JOIN weekly_performance wp
    ON wp.candidate_id = c.candidate_id
LEFT JOIN course co
    ON co.course_id = wp.course_id
LEFT JOIN course_staff_junc csj
    ON csj.course_id = co.course_id
LEFT JOIN staff s
    ON s.staff_id = csj.staff_id;
