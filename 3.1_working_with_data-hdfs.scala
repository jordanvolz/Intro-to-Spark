// ****************************
// 3.1 Working with Data - HDFS
// ****************************

/*
 Let's see how to read data from HDFS. We have some data stored in HDFS under 
 /tmp/BasketballStats_csv. Let's load that into spark. 
*/

val fileName="/tmp/BasketballStats_csv/*"
val bballTxt=spark.read.text(fileName)

/* 
bballTxt is a Dataset. Since we specified that it is a text file, each record is a String
*/

bballTxt  
  
/*
 Once you have a Dataset, there are many things you can do with it. 
 We'll dive into these more in a later section, but here are a few quick examples
*/

// count the number of rows
  
bballTxt.count()
  
// show the first row
  
bballTxt.head()
  
// show the first 10 rows
  
bballTxt.show(10)
bballTxt.take(10).foreach(println)
  
/*
 You can also get a fair amount of information from the Dataset itself. 
 For example. Let's say we're interested to know where the data came from
*/

bballTxt.inputFiles.foreach(println)
  
/*
 We know that our Dataset corresponds to a partition of data on our worker nodes.
 You can check how many partitions are used by your Dataset at any time. 
*/

bballTxt.rdd.getNumPartitions

/*
 One of the ways to control parllelism is by repartitioning your Dataset. 
 I actually have 3 worker nodes, so I want to increase the number of partitions. 
*/

val newbballText = bballTxt.repartition(3)  
newbballText.rdd.getNumPartitions

/*
 You can read data in different formats into spark. 
 Here's an example of reading in a csv. 
*/

val bballCsv=spark.read.
  option("inferSchema", true).
  option("header", true).
  csv(fileName)

/*
 The csv reader reads each entity between commas as its own field
 intead of reading in the entire line as a single field.  
*/

bballCsv
  
/*
 We can interact with this like a table. 
 We'll see how to work with tables more in an upcoming section.
 For Now, let's print the schema. 
*/

bballCsv.printSchema
  

// And we get something more workable when we display the data: 
  
bballCsv.show()
  
/* 
 After we've done our modifications to a Dataset, we may want to save it back to HDFS. 
 It's simple to save your file 
*/

bballTxt.write.
  mode("Overwrite").
  text("/tmp/delete_me/basketball")
  
/* 
 Here we saved as a text file in HDFS. Similarly to how you can read in 
 many different types of files, you can also save to different file types, 
 parquet, hive tables, etc. 
*/