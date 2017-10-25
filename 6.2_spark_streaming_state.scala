// ****************************
// 6.2 Spark Streaming Tracking State
// **************************** 
  
/*
 You may have noticed that in the previous example, we had to set a checkpoint diretory for the 
 streaming context. The checkpoint is used to save intermediate batches so that we can reliably 
 perform window operations on them. Recall that spark works by lineage, and since each window may depend
 on the one before it, this would create arbitrary long dependencies in the case of a fault + recovery. 
 We'll talk more about memory considerations in an upcoming section.
  
 The last type of Streaming operation we'll look at is working with state. It's ofte helpful to 
 have an idea of state in Streaming applications for tracking various values. For example, maybe you're
 looking at a stream of log files and you want to track the number of failures coming through. In these
 cases, it's nice to be able to have a way to remember those values via state. 
  
 We'll show an example here of working with the updateStateByKey function in Spark
 Streaming, that allows you to track state in your stream. 

 We first need to define a function that tracks state. This one simpley keeps
 a running total of the count, adding new counts to it when we get them. We have to use
 an option object instead of a normal Int because the first time we run the function
 we will not have a running count. We deal with this by using a getOrElse(0), which 
 will return 0 in that case. 
*/

val updateTotalCount = (newCounts: Seq[Int], runningCount: Option[Int]) => {
  val previousCount=runningCount.getOrElse(0)
  val currentCount = newCounts.sum
  Some(previousCount + currentCount)
    
}

import org.apache.spark.streaming._
  
val ssc = new StreamingContext(sc, Seconds(5))
  
ssc.checkpoint("/tmp/ssc")
  
val lines = ssc.socketTextStream("cdsw-demo-5.vpc.cloudera.com",9999)

// let's count the value of cpu system time, this just gives us an 
// arbirtary keythat we can use to update by. 
val cpuSysTime=lines.map(line => line.split("\\s+")).
  map(line => (line(13),1))

// run the update function through updateStatebyKey
val countStream=cpuSysTime.updateStateByKey(updateTotalCount)

// get print out all the values of the state
countStream.print()
  
ssc.start()  
ssc.awaitTermination()  