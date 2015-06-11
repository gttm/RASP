CREATE TABLE "netdata" (
    "time" BIGINT PRIMARY KEY,
    "d"."ipS" VARCHAR,
    "d"."ipSI" BIGINT,
    "d"."ipD" VARCHAR,
    "d"."ipDI" BIGINT,
    "d"."proto" SMALLINT,
    "d"."portS" INTEGER,
    "d"."portD" INTEGER,
    "d"."size" INTEGER,
    "as"."asS" VARCHAR,
    "as"."asD" VARCHAR,
    "dns"."dnsS" VARCHAR,
    "dns"."dnsD" VARCHAR
) 
SALT_BUCKETS=4,
DEFAULT_COLUMN_FAMILY='d',
DATA_BLOCK_ENCODING='NONE',
COMPRESSION='SNAPPY';
