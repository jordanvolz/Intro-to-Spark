// ****************************
// 6.4 Structured Streaming
// ****************************

/* 
 Structured Streaming is a newer form of Streaming in Spark. It is still considered
 experimental and is not yet supported by Cloudera or recommend for production. 

 Whereas Spark Streaming is built around RDDs, Structured Streaming is built around 
 Dataframes/Datasets. This has some performance operations (taking advantage of optimizations
 in SparkSQL), as well as more advanced windowing + state capabilities (dealing with late events, etc). 

 As everything is represented as structured data with Structured Streaming, it's relatively
 simple from a coding standpoint to do things like take a average of a value in a stream, 
 or counting a value, etc. These boil down to simple sql actions on a stream, and the Structured
 Streaming frameworks understand how to update this as more data fills the stream. It's also
 better able to handle late arriving data as well.

 We'll build out some examples in this space when we are closer to Structured Streaming being supported
 by Cloudera. For now, feel free to experiment with it on your own.


https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
*/
