// ****************************
// 8.2 Memory Management
// ****************************

/*
 Spark contains many tools for working with memory in the system and
 inteliigently managing your workflows. We'll look at three of them in this
 seciton: caching, broadcasting, and checkpointing. 

 Caching is an important concept that allows you to pin a dataset in memory in
 Spark. This can have a lot of benefits to your workflow and should be used whenever
 you know that a Dataset is going to be reused many times later on. Instead
 of having to recalculate the Dataset every time it is involved in a DAG, we can 
 cache it to short-circuit the calculation. This is particularly useful in 
 iterative workloads. 
*/

// Caching data is a very simple thing to do:

val bballDF = spark.read.
  table("basketball.players")
  
bballDF.persist()

// You can remove from cache via: 
  
bballDF.unpersist()
  
/*
 When you cache a Dataset, you can specify where it gets cached. 
 By default, Spark will cache it in memory (MEMORY_ONLY), but you can also 
 specify MEMORY_AND_DISK (stores to disk if memory is full), DISK_ONLY, 
 MEMORY_ONLY_SER (serialized in memory), MEMORY_AND_DISK_SER. You can also specify
 a number after the storage format, like MEMORY_ONLY_2, which will replicate
 the cache on the specified number of nodes. This helps with reliability, but uses
 greater memory resources when caching data. It should be noted that Spark will
 rebuild the cache if it is lost, but it does take time. Replicating the cache can 
 be good for low SLA use cases. 
  
 In certain use cases, you may want to make a Dataset available locally to every node in the cluster. 
 For example, you can avoid shuffles in a join involving a large table and a smaller table by copying the
 smaller table to every node in the cluster. This can be particularly effective for tables commonly used as 
 reference data or lookup tables. Spark allows you to broadcast a variable out to the cluster, which places a 
 copy of it on every node. This allows you to easy make that data available to every node in the cluster. 
*/

// Broadcasting is also a simple operation: 
  
val broadcastBball = sc.broadcast(bballDF)
  

/*
 The last thing we'll look at is checkpointing. We've already seen this a little with Spark
 Streaming, but checkpoints allow you to truncate the DAG lineage in Spark by saving an intermediate
 Dataset to disk. In the case that a Dataset is the product of hundreds or thousands of transformations,
 checkpointing can help prevent long wait times in case of a failure where the Dataset would need to be 
 recomputed. In the case that Spark jobs create a long chain on a single Dataset, it is a good pratice to 
 periodically checkpoint your Datasets. 
  
 To checkpoint, you only need to set a checkpoint directory, and then you can call checkpoint() on any 
 Dataset afterward. 
*/  
sc.setCheckpointDir("/tmp/ssc")

bballDF.checkpoint()
  
