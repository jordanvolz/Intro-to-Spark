// ****************************
// 3.0 Working with Data
// ****************************

/*
 Now that we know how to create a SparkSession, we're ready to start working with data.
 Before we do so, it's good to have an understanding of how Spark deals with data. 
 There are 3 types of data interfaces in Spark: RDDs, DataFrames, and Datasets. 

 RDDs (Resilient Distributed Dataset) was the first data type. 
 An RDD is a partitioned  data set that is distributed across nodes in the spark cluster. 
 Spark then performs actions on these partitions in parallel tasks that run on each node.
 RDDs can store any type of data and are very flexible. 

 DataFrame was the second type of data interface. 
 A DataFrame was originally defined as a a collection of distributed Row types, thereby 
 giving the data set structure, like a table. 
 DataFrames are the backbone to sparkSql and is often how users interact with data in Spark. 


 Datasets are a new interface that are a superset of RDDs and Dataframes. They are the main data 
 interface in Spark 2.x. Datasets are a typed interface, which provides many benefits to working 
 with data in Spark, such as runtime type-safety and easier APIs.

 RDDs are still the lowest level interface, and you can always drop down into an RDD if necessary. 
 This can be useful for code that was written in Spark 1.6 that has incompatibilities with spark 2.x
 (Although you may sacrifice some Dataframe optimizations).
 With the new Dataset interface, you can view a DataFrame as a Dataset of type Row and a rdd as a Dataset 
 of type Object. 

![datasets](./images/dataset.png)
*/


