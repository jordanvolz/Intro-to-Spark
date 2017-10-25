// ****************************
// 5.0 SparkSQL
// ****************************

/*
 SparkSQL is a module in Spark built for structured data. Working 
 with structured data (i.e. Dataframes) allows spark to have more information about the 
 data, and Spark is better able to optimize spark code when run via SparkSQL
 It's recommended to try to use SparkSQL whenever possible to gain the 
 benefit of these optimizations. 

 There are two main ways to interact with SparkSQL, either via the API or 
 by executing SQL queries. We'll start by looking at SQL queries. 

 You can directly issue spark queries via the spark.sql command. This will return  
 a Dataframe to spark. 
*/

val bballDF = spark.sql("SELECT * from basketball.players")
  
bballDF.show(5)
  
/*
 Here, we are reading data out of Hive. Spark also can keep track of temporary tables, 
 which you can pin in memory with createOrReplaceTempView (registerTempTable in spark 1.x). 
 For those more familiar with SQL than the sparkSQL API, this provides a good way for you
 to interact with your structured data. You can execute complex commands here given the full
 creativity of the SQL syntax. 
  
 The SparkSQL API can be very elegant. We'll show some examples below of how you can leverage it, 
 using our favorite dataset. 
*/

//our table is way too wide, let's just select a small amount to view
val bballDFSmall=bballDF.
  select($"name",$"age",$"year",$"team",$"pts",$"zTot")

bballDFSmall.show(20)

//Let's try some aggregations
bballDFSmall.groupBy($"age").
  count().sort($"age").show
  
val bballDFAvgZtot=bballDFSmall.groupBy($"name").
  avg("zTot").sort($"avg(zTot)" desc)

bballDFAvgZtot.show

bballDFSmall.groupBy($"name").avg("zTot").
  sort($"avg(zTot)" asc).show
  
//We can also filter

bballDFSmall.where($"team" === "CHI").
  sort($"zTot" desc).show

bballDFSmall.where($"pts" > 30).
  sort($"pts" desc).show
  
//and of course joining datasets
  
val bballJoined=bballDFSmall.
  join(bballDFAvgZtot,bballDFSmall("name") === bballDFAvgZtot("name")).
  select(bballDFSmall("name"),$"age",$"year",$"team",$"pts",$"zTot",$"avg(zTot)").
  withColumn("zDiff", $"zTot"-$"avg(zTot)")

bballJoined.show
  
/*
 Last we'll look at creating temporary tables. It's pretty easy and then allows you to access
 them with normal SQL syntax
*/

bballJoined.createOrReplaceTempView("basketballJoined")

spark.sql("Select * from basketballJoined where name = 'James Harden'").show