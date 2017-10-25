// ****************************
// 8.3 Monitoring Spark Jobs with Spark UK
// ****************************

/*
Note: It helps to reset the SparkSession for this part. 

 So, we now have a pretty good understanding on how 
 to use Spark and how to accomplish many different use
 cases with many different types of data! We can start to 
 feel the possibilities! 

 A question arises: How do you monitor Spark? How do you 
 troubleshoot when things go wrong. This is a much larger topic
 than we can cover here, and indeed, I propose a separate talk
 all together to dive into some of the specifics of that. For now, 
 we'll mention some ways that you can get some good information from
 spark that may be of use when trying to figure out how things are working. 

 First of all, the Spark UI is a great place to get information on what's
 going on in your spark job. If you're a  CDSW user, we link you to the UI in
 your existing job:

![CDSW Spark Links](./images/ui_1.png)

 What's the difference between these? The Spark History Sever UI
 contains links to all previous Spark jobs, whereas the Spark UI 
 is a direct link to the SparkSession you are currently running. 
 You can of course get to the current job from the Spark History UI, 
 but for convenience we link you directly. Note that if you're running
 CDSW code as a job, it's a good idea to print out the application id,
 as previously shown ,so that you can look it up later. 

![Spark Links](./images/ui_2.png)

 If you're not using CDSW, the application web UI can be accessed
 via the http://<driver-host>:4040, although if there are multiple Spark
 contexts running, sesequent UIs will be on sequential ports -- 4041, 4042, etc
 To find the Spark History Server, you can find it linked on the Spark
 service page in Cloudera Manager. 

![CM History Link](./images/ui_3.png)

 Let's dig into the application UI. If we go there right now, there is nothing 
 intersting, as we haven't done anything yet.

![Spark App 1](./images/ui_4.png)

 Let's do something simple like read a table and count the lines to see what is
 picked up in the UI. 
*/

val bballDF = spark.read.table("basketball.players")
bballDF.persist()
bballDF.count()
  
/*
![Spark App 2](./images/ui_5.png)

  
 Now we see that we have 1 job! We see that it corresponds to a count() action as well, 
 which lines up with what we did in the code. Recall that a job lines up with a Spark action:
 Spark performs lazy evaluations and only actually performs any computation when you ask for a 
 result. It will pipeline all your transformations together into stages and separate the work
 on executors as tasks. We get some high level info here like how many stages and tasks were
 involved in a job and how long it took, etc. You can also expand the event timeline to see when
 executors are added and dropped from your application. 
 We can click into a job to get information on the stages and tasks.  

![Spark Job 1](./images/ui_6.png)

 Here we can see the two stages for the job. We can expand the event timeline to get a view of 
 when executors are added/deleted from the job, and the DAG visualization gives an overview of how
 Spark processed our code. Clearly, in this example we have two stages. We get high level info on each
 stage, such as how long it took, how many tasks were involved, how much data was read from/written to hdfs, 
 and how much data is shuffled between stages. Our example caused two stages, as each executor will 
 perform a local count, and then there is a shuffle to aggregate the data and perform the final count. 
 This is a good spot to locate if there is a stage in your job that is taking up a lot of time. Are there any
 steps in your job that you anticipate taking up large amounts of time/resources? 
  
 We can further drill into a stage by clicking on one of them. 

![Spark Stage 1](./images/ui_7.png)
  
 At the top we see a more detailed version of the DAG for this stage. This gets into more details on the types of RDDs
 being used under the hood, but it requires a more advanced understanding of Spark to unravel this than we'll expect 
 for this tutorial. 
  
![Spark Stage 2](./images/ui_8.png)
  
  
 The next section show summary statics for the tasks in the job, such as duration, peak memory use, shuffle size, etc.
 One of the things this is useful for is detecting skew in your job. I.E. in a well designed spark job on a distributed 
 system, we would expect most tasks to complete in roughly the same amount of time. If we notice wild deviations (very 
 different max/min, for example) this may be an indication that something strange is going on with our job. 
  
 The section after that shows an aggregate view of the executors. Here we get info on how many tasks the executor ran, how many failed, 
 how long tasks too, etc. This view is good at giving us information if we're having issues with paritcular exeuctors. 
 I.E. if we have a bad host where executors are all failing, we'll be able to detect that here (in such a scenario, Spark will eventually blacklist
 the node, but it's still nice to have this information recorded!)
 

![Spark Stage 3](./images/ui_9.png)


 The last section shows a view of each task in the stage, along with useful statistics. You can use this to further montior and troubleshoot
 when necessary. For the executor and task views, Spark links to the stderr and stdout files from the YARN containers. These are valuable when things
 go wrong, as many errors will be captured by YARN (OOM, for example). The Task view will also bubble up errors in the last column, but it's
 often useful to have a link to the actual YARN log (Note: these log files are not available until after you stop your context). 
  
 The application UI contains 6 tabs. Our explorations has also taken us through 3 of them: 
 Jobs, Stages, and Tasks. We'll talk about the other 3 now. 
  

![Spark Environment ](./images/ui_10.png)

  
 The Spark Environment tab contains all the settings and configuration relative to your spark application. Any config set in your job will be displayed here. 
 This captures exactly what configurations were applied to your job, and can be super helpful as you tune jobs to figure out what settings applied to a certain
 run, etc. 
  

![Spark Storage 1](./images/ui_11.png)

  
 The storage tab shows how much storage the job is taking up. Notice that we persisted our DataFrame in the code above. As a result, we see that
 Spark is using 5.3 MB of space to keep that in memory (It's a pretty small table!). If we unpersist our Dataframe, 
 we free up the memory, as shown below. 


![Spark Storage 2](./images/ui_12.png)

  
 Lastly, there is the SQL tab. This tab is new for Spark 2, and it includes more details about how Spark plans and executes operations
 on Dataframes. As many of the optimizations in Spark are with regard to Dataframes, it can be useful to see how Spark is translating your commands. 
 As this is a newer feature in Spark, don't be suprised if changes in future releases. 
*/

bballDF.groupBy($"name").avg("zTot").sort($"avg(zTot)" asc).show

/*
![Spark SQL 1](./images/ui_13.png)

  

![Spark SQL 2](./images/ui_14.png)
*/

  
  