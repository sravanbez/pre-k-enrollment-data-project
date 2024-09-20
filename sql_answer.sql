--Provide a SQL query that will answer this question: 
SELECT state AS state_name, SUM(enrollment) AS total_enrollment
FROM spectrum_schema.pre_k_enrollment_data
WHERE year = 2021
GROUP BY state
ORDER BY total_enrollment DESC
LIMIT 10;
