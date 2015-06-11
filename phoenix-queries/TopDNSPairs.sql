SELECT "dns"."dnsS", "dns"."dnsD", COUNT(1) AS "pairCount" 
FROM "netdata" 
WHERE "time" > 1434017860
GROUP BY "dns"."dnsS", "dns"."dnsD" 
ORDER BY "pairCount" DESC 
LIMIT 10;