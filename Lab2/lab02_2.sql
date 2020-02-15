-- Databricks notebook source
select City,IATA,count(*) as total_flights from((select City,IATA,date from departuredelays join airportcodes on origin=IATA) union (select City,IATA,date from departuredelays join airportcodes on destination=IATA)) where substring(date,0,2)='03' group by City,IATA order by total_flights desc limit 1;

-- COMMAND ----------

select * from (select City,IATA,substring(date,0,2) as month,count(*) as arrival_count from departuredelays join airportcodes on origin=IATA group by City,IATA,substring(date,0,2)) natural join (select City,IATA,substring(date,0,2) as month,count(*) as departure_count from departuredelays join airportcodes on destination=IATA group by City,IATA,substring(date,0,2))

-- COMMAND ----------

select substring(date,0,2) as month,avg(delay) as avg_delay from departuredelays group by month;


-- COMMAND ----------

drop table if exists airportcodes;
create table airportcodes using csv options(path "/databricks-datasets/flights/airport-codes-na.txt", header "true", delimiter "\t", inferSchema "true");
select * from airportcodes limit 5;

-- COMMAND ----------

drop table if exists departuredelays;
create table departuredelays using csv options(path "/databricks-datasets/flights/departuredelays.csv", header "true", inferschema "true");
select * from departuredelays limit 5;

-- COMMAND ----------

drop table if exists iot_devices;
create table iot_devices using json options(path "/databricks-datasets/iot/iot_devices.json", header "true", inferschema "true");
select * from iot_devices limit 5;


-- COMMAND ----------

drop table if exists diamonds

-- COMMAND ----------

select color,count(*) from diamonds group by color;

-- COMMAND ----------

select distinct clarity from diamonds

-- COMMAND ----------

select * from diamonds limit 5

-- COMMAND ----------

drop table if exists diamonds;

create table diamonds(_c0 string, carat double,cut string, color string, clarity string, depth double, dtable int,price double, x double,
          y double, z double) using csv options(path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");
describe diamonds;

-- COMMAND ----------

describe diamonds

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;
CREATE TABLE diamonds USING csv
          options (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",header "true", inferschema "true") 
