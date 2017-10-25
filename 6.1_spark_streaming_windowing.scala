// ****************************
// 6.1 Spark Streaming Windowing 
// ****************************

/*
 You may have noticed that we have to shut down the spark-shell (in this case
 CDSW Sesion, in order to stop our Spark Streaming job. Spark Streaming is designed 
 for long running appliations and is not suitable for production streaming jobs. 
 Although it can be suitable for development/testing of streaming, any real streams
 should run as a normal spark application via spark-submit and it is not recommended to use
 spark-shell or CDSW for long-running jobs. We use it here only for purposes of instruction. 
  

 Most normal operations can be applying to DStreams (like map, above), although there are a few special 
 ones that we should mention. Windowing is a common streaming concept that allows you to 
 specify operations over a range of data. In Spark Streaming, you can specify a window to operate
 on a number of RDDs. There are a number of operations, like countByWindow, reduceByWindow,
 countByValueAndWindow, reduceByValueAndWindow, etc. Generally you need to specify the length of
 the window, and the slide interval. Let's look at an example using windowing
  
![streaming image](./images/streaming-dstream-window.png)
*/
  
import org.apache.spark.streaming._
  
val ssc = new StreamingContext(sc, Seconds(1))
  
ssc.checkpoint("/tmp/ssc")
  
val lines = ssc.socketTextStream("cdsw-demo-5.vpc.cloudera.com",9999)
    
val counts=lines.countByWindow(Seconds(5),Seconds(2))
  
counts.print()
  
ssc.start()  
ssc.awaitTermination()  
