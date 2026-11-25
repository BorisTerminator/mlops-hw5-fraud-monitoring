SELECT $__timeGroupAlias(scored_at, '10s'), count(*) / 10.0 AS tps
FROM scores
WHERE $__timeFilter(scored_at)
  AND us_state IN (${us_state:sqlstring})
  AND merch IN (${merch:sqlstring})
GROUP BY 1 ORDER BY 1;
