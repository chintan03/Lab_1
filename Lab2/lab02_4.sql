-- Databricks notebook source
drop table if exists products;
create table products using csv options (path "/FileStore/tables/Products.tsv", header "true", sep "\t", inferSchema "true");
describe products;

-- COMMAND ----------

drop table if exists orderDetails;
create table orderDetails using csv options (path "/FileStore/tables/OrderDetails.tsv", header "true", sep "\t", inferSchema "true");
describe orderDetails;

-- COMMAND ----------

select ProductID,Name,int(Weight) from products order by int(Weight) desc limit 15;


-- COMMAND ----------

select ProductCategoryID,count(*) as total_product from products group by ProductCategoryID;

-- COMMAND ----------

select count(pidcount) from (select distinct ProductModelID as pidcount from products);

-- COMMAND ----------

select ProductModelID,count(ProductID) as total_item from products group by ProductModelID order by total_item desc limit 10;

-- COMMAND ----------

select ProductID,Name,Color,Size,ListPrice from products where ProductModelID=20;

-- COMMAND ----------

select sum(OrderQty) from ((select ProductID from products where ProductCategoryID=20)natural join orderDetails);

