WITH selected AS (
  SELECT LEAST(floor(score * 20)::int, 19) AS bin
  FROM scores
  WHERE $__timeFilter(created_at)
    AND us_state IN (${us_state:sqlstring})
    AND merch IN (${merch:sqlstring})
), bins AS (
  SELECT generate_series(0, 19) AS bin
)
SELECT to_char(b.bin * 0.05, 'FM0.00') AS score_bin,
       count(s.bin)::double precision / GREATEST((SELECT count(*) FROM selected), 1) / 0.05 AS density
FROM bins b LEFT JOIN selected s USING (bin)
GROUP BY b.bin ORDER BY b.bin;
