SELECT "dns"."dnsS", "dns"."dnsD", COUNT(1) AS "pairCount"
FROM "netdata"
WHERE "time" > 1434144920000000 and "time" < 1434146120000000
GROUP BY "dns"."dnsS", "dns"."dnsD"
ORDER BY "pairCount" DESC
LIMIT 10;
