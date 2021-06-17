CREATE VIEW [Course Overview] AS
SELECT
    ca.candidate_id,
    ca.candidate_name,
    ca.degree_result,
    l.[location],
    c.course_id,
    c.course_name,
    c.start_date,
    c.duration,
    wp.week_no,
    wp.analytic,
    wp.independent,
    wp.determined,
    wp.professional,
    wp.studious,
    wp.imaginative,
    ROUND(
        (CAST(sd.presentation AS FLOAT) / CAST(sd.presentation_max AS FLOAT)) 
        * 100, 2
        ) AS presentation_pct,
    ROUND(
        (CAST(sd.psychometrics AS FLOAT) / CAST(sd.psychometrics_max AS FLOAT)) 
        * 100, 2
        ) AS psychometrics_pct
FROM course c
INNER JOIN weekly_performance wp
    ON wp.course_id = c.course_id
INNER JOIN sparta_day sd
    ON sd.candidate_id = wp.candidate_id
INNER JOIN candidate ca
    ON ca.candidate_id = sd.candidate_id
INNER JOIN [location] l
    ON l.location_id = sd.location_id;
