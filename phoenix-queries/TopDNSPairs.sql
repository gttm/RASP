SELECT "sourceDNS", "destinationDNS", COUNT(*) AS "pairCount" 
FROM "output" 
WHERE "dateTime" > '1433690000' AND "dateTime" < '1433693600'
GROUP BY "sourceDNS", "destinationDNS" 
ORDER BY "pairCount" DESC 
LIMIT 10;