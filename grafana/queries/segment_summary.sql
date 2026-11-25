SELECT count(*) AS scores, avg(fraud_flag::double precision) AS flagged_fraction,
       COALESCE(sum(amount), 0) AS turnover,
       COALESCE(sum(amount * fraud_flag), 0) AS flagged_amount
FROM scores
WHERE $__timeFilter(created_at)
  AND us_state IN (${us_state:sqlstring})
  AND merch IN (${merch:sqlstring});
