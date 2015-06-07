-- Create view for existing hbase table
CREATE VIEW "output" (
    "dateTime" VARCHAR PRIMARY KEY, 
    "cf"."sourceIP" VARCHAR, 
    "cf"."sourceIPInt" UNSIGNED_LONG, 
    "cf"."destinationIP" VARCHAR, 
    "cf"."destinationIPInt" UNSIGNED_LONG, 
    "cf"."protocol" VARCHAR, 
    "cf"."sourcePort" VARCHAR, 
    "cf"."destinationPort" VARCHAR, 
    "cf"."ipSize" VARCHAR, 
    "cf"."sourceAS" VARCHAR, 
    "cf"."destinationAS" VARCHAR, 
    "cf"."sourceDNS" VARCHAR, 
    "cf"."destinationDNS" VARCHAR
);