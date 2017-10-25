// ****************************
// 8.1 Spark Configuration
// ****************************

/*
 Spark's internal workings can be quite complicated and have a lot of knobs
 that can be turned at the global or local level. Although a spark user doesn't need
 to know every configuration parameter available, it's helpful to understand
 how you can modify them when needed. 

 Spark Properties refer to the properties applied to a specific spark job. These take the 
 shape of key-value pairs. Typically you set these either with launching an application in 
 spark-shell/spark-submit, or in the SparkSession itself. 

 You can set configuration properties when invoking spark-shell/spark-submit by using the
 --conf switch like this: 
*/ 

// spark-submit --name "My Spark Job" --master yarn 
//   --deploy-mode cluster --queue my_queue
//   --conf "<key>=<value>"
//   myApp.jar


/* These configruations will be applied to the job when it is run and override global settings. 
 You can also directly add configurations to your SparkSession via the following: 
*/

// When creating a SparkSession:
// val spark = SparkSession.builder().appName("My Spark Job").
//    config("<Key>","<value>"").
//    getOrCreate()

// Programmatically: 
// spark.conf.set("<Key>","<value>")

/*
 Special for CDSW users, you can specify configuration parameters via your spark-defaults.conf
 file in the root of your CDSW projects. Parameters take the form of:
 "<key>" "<value>"
 This makes sharing easy, as the creator of the project can specify important parametres like
 core/RAM requirements, needed JARs, etc, and others can copy the project and not worry about getting
 all that information correct. 

 Note, you can always check the settings for your SparkSession via
 spark.conf.getAll() or by looking at the "Environment" tab in your Spark UI,
 which we'll discuss shortly. 

 What do you have access to configure? Lots! Way too much to cover here. 
 Although stuff like executor/driver core and RAM use is pretty crucial to running your job smoothly, 
 there are lots of other configuraitons which may be important depending on your use case. We recommend
 the offical guide for more detail. 
  
 https://spark.apache.org/docs/latest/configuration.html


 The last thing we'll mention is that with CDH, Spark runs on YARN and the YARN settings can affect the 
 performance of Spark jobs quite a bit. It's always adviseable to ensure that YARN is properly tuned. Typically,
 YARN settings should be controlled via Cloudera Manager.
*/