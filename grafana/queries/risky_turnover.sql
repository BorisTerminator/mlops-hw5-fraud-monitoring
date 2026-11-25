SELECT $__timeGroupAlias(created_at, '30s'),
       sum(amount) AS turnover,
       sum(amount * fraud_flag) AS flagged_amount
FROM scores
WHERE $__timeFilter(created_at)
  AND us_state IN (${us_state:sqlstring})
  AND merch IN (${merch:sqlstring})
GROUP BY 1 ORDER BY 1;
