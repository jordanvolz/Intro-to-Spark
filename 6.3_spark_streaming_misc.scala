// ****************************
// 6.3 Spark Streaming Misc
// **************************** 
  

/* 
 Note that Spark 1.6 introduce a new function, mapWithState, which should be more 
 performant than updateStateByKey. Currently this is an experimental function and is
 not recommended for production use, though it may be more suitable than updateStateByKey
 in the future.   
  
 We'll wrap up with a few notes here. Kafka is one of the most used tools to integrate with 
 Spark Streaming. I'm too lazy to set up a kafka broker to show as an example (note: do this
 later when you have more free time!), but here is some sample code which shows how to create a 
 DStream using Kafka as the source. From there it's easy to work with the incomign Kafka
 data like any other data with Spark. 
*/

// val kafkaStream = KafkaUtils.createDirectStream[String, String](
//     ssc,
//     <map_of_kafka_params>,
//     <set_of_topics_to_consume>
// )

/*
 Also note that since the Kafka API changed between v0.8 and v0.10, you'll need different 
 packages for communicating with brokers in 0.8 + 0.10. The Kafka Integration Guide has much more detail
 on this. https://spark.apache.org/docs/latest/streaming-kafka-integration.html

 If  you are working securely between Kafka and Spark Streaming, the following blog
 will likely be useful: https://blog.cloudera.com/blog/2017/05/reading-data-securely-from-apache-kafka-to-apache-spark/
*/
  
/*
 It's common in stream processing to want to save data out to an external system. Some of these are natively integrated
 with spark, such as HDFS, but others require external packages, like Kudu. In the latter instance, 
 it's pretty common to have to create connection objects in order to communicate with them. When working in 
 a stream, this can be a little tricky. Since we are in a distributed system, we can't share the connection object
 between nodes, but we also don't want to impact performance by creating too many connection objects. The
 right balance here is to utilize the foreachRDD and foreachPartition commands as follows: 
*/

// dStream.foreachRDD { rdd => //RDDs are distributed, not the righ place to create a connection object
//    rdd.foreachPartition => { partitionOfRecords //a parition lives on a single node, bingo!
//        val connection = new ConnectionObject(<params)
//        partitionOfRecords.foreach (record => connection.send(record)) //send individual records, or in a batch, etc
//          connection.close()
//     }
// }
  
