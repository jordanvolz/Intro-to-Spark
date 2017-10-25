// ****************************
// 3.2 Working with Data - HDFS
// ****************************

/*
 Working with data in Hive is very simple with the SparkSession. 
 In Spark 1.x, there was a HiveContext for working with Hive and a 
 sqlContext for general sparkSQL use, which caused some confusion. 
 Now you only need to work with the SparkSession, and it's very easy to 
 pull data out of hive. 
*/

/*
 We have a table in hive "basketball.players". That has our basketball 
 data in it. Let's pull it into Spark.
*/

val bballDF = spark.read.
  table("basketball.players")
  
bballDF
  
bballDF.printSchema
  
bballDF.rdd.getNumPartitions

val newbballDF = bballDF.repartition(3) 
  
/*
 We'll dive more into the sparkSQL API in another section. 
  
 Similarly to working with data in HDFS, you can also save your data
 to Hive with a few quick commands. 
*/

newbballDF.write.
  mode("Overwrite").
  saveAsTable("basketball.players_delete_me")
  
val bballDFdeleteme = spark.read.
  table("basketball.players_delete_me")
  
bballDFdeleteme.count()
  
/*
 Note that when creating tables, you can specify format, partitions, buckets, etc. 
 Conveniently, spark uses the parquet format by default, which is recommended by Cloudera. 
  
 With Spark, you have a lot of control over your Hive Data. 
 You can create and delete tables, etc.
*/

spark.sql("DROP TABLE basketball.players_delete_me")
  
val bballDFdeleteme = spark.read.
  table("basketball.players_delete_me")

/*
 We got an error on that last action, since we deleted the table
 before we tried to read it. This confirms that the table was actually 
 dropped!
*/
  

  
  