// ****************************
// 6.0 Spark Streaming 
// ****************************

/* 
 Spark Streaming is an extension of Spark designed to work with 
 streaming data. This enables spark in the use of near real-time (NRT)
 systems and use cases. Spark Streaming connects with other aspects of spark -- 
 core, sql, ML, etc -- so you can get the most out of your streaming data
 with the full power of spark to process, analyze, and model data. 

 Spark Streaming has connections already built for popular streaming services like 
 Kafka + Flume, which constitutes many of the Big Data use cases on CDH. There are also 
 many third-party connectors built using Spark Streaming custom receiver API, which
 can be used to help connect spark to different tools. 
 As you may expect, once the data is in spark, it's easy to transform the data and then
 persist it in your storage layer of choice -- HDFS, HBase, Kudu, S3, etc. 

 Let's first take a high level overview of how Spark Streaming works before we 
 dive into an example. Spark Streaming works by taking your input stream and 
 breaking it up into small batches of data. Each batch is an RDD, and these 
 batches are known as DStreams, a sequence of RDDs. The batching process is referred to as
 microbatching.  

 What's nice about DStreams is that it passess RDDs downstream, which we already know 
 how to work with in Spark. Spark Streaming takes a complex problem (Streaming), and allows
 us to work with it in the same way we would any other spark job. 


![spark streaming overview](./images/streaming-flow.png)

   
 Let's look at an example of how we can use spark streaming. 

 First let's set up a stream. We'll do something simple and set up a TCP stream
 on one of our nodes. Log into a node in the system and run the following command:
*/

// vmstat -n 1 | nc -lk 9999 &

/*
 This sends the results of vmstat -n 1 to port 9999. 

 Now we create our spark streaming context
*/

import org.apache.spark.streaming._
  
val ssc = new StreamingContext(sc, Seconds(1))

/*
 We now create a stream w/ our streaming context. In this example we create a 
 socketTextStream, using port 9999 on the host we chose. 
*/

val lines = ssc.socketTextStream("cdsw-demo-5.vpc.cloudera.com",9998)
  
// We'll do a simple command here -- just pull out the free memory and print it. 
  
val memFree=lines.map(line => line.split("\\s+")(4))

// We could save our info to HDFS, etc, but we'll simply just print it out instead.   

memFree.print()
  
// Now we start our spark job!
  
ssc.start()  
ssc.awaitTermination()
  



  

