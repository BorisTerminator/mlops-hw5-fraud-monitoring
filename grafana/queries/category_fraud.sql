WITH recent AS (
  SELECT * FROM scores ORDER BY id DESC LIMIT 1000
)
SELECT cat_id, avg(fraud_flag::double precision) AS flagged_fraction
FROM recent
WHERE us_state IN (${us_state:sqlstring})
  AND merch IN (${merch:sqlstring})
GROUP BY cat_id ORDER BY flagged_fraction DESC;
