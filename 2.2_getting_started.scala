// ****************************
// 2.2 Getting Started in Spark-Shell for Scala Users 
// ****************************

/*
For scala users, spark-shell comes preloaded with your SparkSession
 as spark, and sparkContext, as sc. You don't need to initialize anything 
 and can just access them directly. 
*/  
spark

sc
  
spark.sparkContext == sc
  
sc.applicationId