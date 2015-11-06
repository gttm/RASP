SELECT "as"."asS", "as"."asD", COUNT(1) AS "pairCount"
FROM "netdata"
GROUP BY "as"."asS", "as"."asD"
ORDER BY "pairCount" DESC
LIMIT 10;
