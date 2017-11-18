-- table containing the data
create external table airports (
    iata string,
    airport string,
    city string,
    state string,
    country string,
    geolat float,
    geolong float
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
location '/user/okmich20/proto/hive/airports';

-- the scd2 table
create external table airport_dim (
	iata string,
	airport string,
	city string,
	state string,
	country string,
	geolat float,
	geolong float,
	start_date date,
	end_date date,
	active boolean
)
stored as parquet
location '/user/okmich20/proto/hive/airport_dimension';

--load data into scd table
load data inpath '/user/okmich20/proto/data/first_batch.csv' overwrite into table airports;

--create staging 
create table airport_staging 
stored as parquet
as select * from airport_dim limit 0;

-- copy into staging all records in target that are not in source
insert into airport_staging 
select * from airport_dim where iata not in (select iata from airports);

-- copy into staging all records that are in source but not in staging and set defalts
insert into airport_staging 
select *, current_date(), null, true from airports where iata not in (select iata from airport_dim);

-- copy into staging all records in both stable but match 
insert into airport_staging
select a.* from airport_dim a join airports b on a.iata = b.iata
where a.airport = b.airport
and a.city = b.city
and a.state = b.state
and a.country = b.country;


-- copy into staging all records in both stable but dont match from the target 
-- make sure to set end_date and active records to mark their time in history
insert into airport_staging
select a.iata, a.airport, a.city, a.state, a.country, a.geolat, a.geolong, 
a.start_date, current_date(), false from airport_dim a join airports b on a.iata = b.iata
where a.airport = b.airport
or a.city = b.city
or a.state = b.state
or a.country = b.country;

-- copy into staging all records in both stable but dont match from the source 
-- make sure to set start_date and active records to mark them as current versions
insert into airport_staging
select a.*, current_date(), null, true from airports a join airport_dim b on a.iata = b.iata
where a.airport = b.airport
or a.city = b.city
or a.state = b.state
or a.country = b.country;

-- insert overwrite from staging to target
insert overwrite table airport_dim
select * from airport_staging;

-- drop the staging table
drop table airport_staging;


load data inpath '/user/okmich20/proto/data/second_batch.csv' overwrite table airports;


--create staging 
create table airport_staging 
stored as parquet
as select * from airport_dim limit 0;

-- copy into staging all records in target that are not in source
insert into airport_staging 
select * from airport_dim where iata not in (select iata from airports);

-- copy into staging all records that are in source but not in staging and set defalts
insert into airport_staging 
select *, current_date(), null, true from airports where iata not in (select iata from airport_dim);

-- copy into staging all records in both stable but match 
insert into airport_staging
select a.* from airport_dim a join airports b on a.iata = b.iata
where a.airport = b.airport
and a.city = b.city
and a.state = b.state
and a.country = b.country;


-- copy into staging all records in both stable but dont match from the target 
-- make sure to set end_date and active records to mark their time in history
insert into airport_staging
select a.iata, a.airport, a.city, a.state, a.country, a.geolat, a.geolong, 
a.start_date, current_date(), false from airport_dim a join airports b on a.iata = b.iata
where a.airport = b.airport
or a.city = b.city
or a.state = b.state
or a.country = b.country;

-- copy into staging all records in both stable but dont match from the source 
-- make sure to set start_date and active records to mark them as current versions
insert into airport_staging
select a.*, current_date(), null, true from airports a join airport_dim b on a.iata = b.iata
where a.airport = b.airport
or a.city = b.city
or a.state = b.state
or a.country = b.country;

-- insert overwrite from staging to target
insert overwrite table airport_dim
select * from airport_staging;

-- drop the staging table
drop table airport_staging;
