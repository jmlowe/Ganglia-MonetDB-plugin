SET SCHEMA "sys";
CREATE TABLE "sys"."disk_total" (
	"disk_total" double,
	"ts" TIMESTAMP,
	"nodename" varchar(128)
);
CREATE TABLE "sys"."floats" (
	"metric" varchar(64),
	"val" double,
	"ts" TIMESTAMP,
	"nodename" varchar(128)
);
CREATE TABLE "sys"."doubles" (
	"metric" varchar(64),
	"val" double,
	"ts" TIMESTAMP,
	"nodename" varchar(128)
);
CREATE TABLE "sys"."smallints" (
	"metric" varchar(64),
	"val" smallint,
	"ts" TIMESTAMP,
	"nodename" varchar(128)
);
CREATE TABLE "sys"."ints" (
	"metric" varchar(64),
	"val" int,
	"ts" TIMESTAMP,
	"nodename" varchar(128)
);
CREATE TABLE "sys"."bigints" (
	"metric" varchar(64),
	"val" bigint,
	"ts" TIMESTAMP,
	"nodename" varchar(128)
);
SET SCHEMA "sys";
