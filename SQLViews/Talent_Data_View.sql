CREATE VIEW [Talent Data] AS
SELECT 
    s.staff_id,
    s.staff_name,
    sd.result,
    l.[location],
    sd.course_interest,
    ROUND(
        (CAST(sd.presentation AS FLOAT) / CAST(sd.presentation_max AS FLOAT)) 
        * 100, 2
        ) AS presentation_pct,
    ROUND(
        (CAST(sd.psychometrics AS FLOAT) / CAST(sd.psychometrics_max AS FLOAT)) 
        * 100, 2
        ) AS psychometrics_pct
FROM staff s
INNER JOIN candidate c
    ON c.staff_id = s.staff_id
INNER JOIN sparta_day sd
    ON sd.candidate_id = c.candidate_id
INNER JOIN [location] l
    ON l.location_id = sd.location_id;
