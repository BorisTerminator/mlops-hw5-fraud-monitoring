SELECT model_version, count(*) AS scores,
       avg(fraud_flag::double precision) AS flagged_fraction,
       avg(score) AS mean_score, max(created_at) AS latest_write
FROM scores WHERE $__timeFilter(created_at)
GROUP BY model_version ORDER BY latest_write DESC;
