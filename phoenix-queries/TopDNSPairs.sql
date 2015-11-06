SELECT "dns"."dnsS", "dns"."dnsD", COUNT(1) AS "pairCount"
FROM "netdata"
GROUP BY "dns"."dnsS", "dns"."dnsD"
ORDER BY "pairCount" DESC
LIMIT 10;
