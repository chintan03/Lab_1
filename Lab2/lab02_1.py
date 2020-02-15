# Databricks notebook source
data=[1,2,3,4,5]
distdata=sc.parallelize(data)
print(distdata.collect())

# COMMAND ----------

testRDD=sc.textFile("/FileStore/tables/check.txt")
lines=testRDD.filter(lambda x: len(x)>0).flatMap(lambda x: x.split(" "))
wordcount=lines.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).sortByKey(True)
print(wordcount.collect())

# COMMAND ----------

from pyspark.sql import *
department = Row("dno","name")
department1 = department("123","CS")
department2 = department("456","ME")
department3 = department("789","CE")
department4 = department("101","T&D")

employee = Row("ssn","firstname","lastname","email","salary","dno")
employee1 = employee("123-123-123","xyz","abc","abc@gmail.com","1000000","123")
employee2 = employee("456-456-456","lmn","dkdv","def@gmail.com","1000000","456")
employee3 = employee("789-789-789","sdv","asdf","ghi@gmail.com","1000000","789")
employee4 = employee("101-101-101","sdd",dfgf","jkl@gmail.com","1000000","101")

# COMMAND ----------

department_list = [department1,department2,department3,department4]
df1 = spark.createDataFrame(department_list)
df1.show()
employee_list = [employee1,employee2,employee3,employee4]
df2 = spark.createDataFrame(employee_list)
df2.show()

# COMMAND ----------

df1.join(df2,df1.dno==df2.dno,'inner').show()

# COMMAND ----------

df1.registerTempTable('Emp')
df2.registerTempTable('Dep')
sqlContext.sql("select * from Emp join Dep on Emp.dno==Dep.dno").show()

# COMMAND ----------

diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",header="true",inferSchema="true")
diamonds.write.format("delta").save("/delta/diamonds")
diamonds.show()
