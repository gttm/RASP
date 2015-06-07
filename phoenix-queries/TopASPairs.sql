SELECT "sourceAS", "destinationAS", COUNT(*) AS "pairCount" 
FROM "output" 
WHERE "dateTime" > '1433690000' AND "dateTime" < '1433693600'
GROUP BY "sourceAS", "destinationAS" 
ORDER BY "pairCount" DESC 
LIMIT 10;