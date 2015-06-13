SELECT "as"."asS", "as"."asD", COUNT(1) AS "pairCount"
FROM "netdata"
WHERE "time" > 1434144924000000 and "time" < 1434146124000000
GROUP BY "as"."asS", "as"."asD"
ORDER BY "pairCount" DESC
LIMIT 10;
