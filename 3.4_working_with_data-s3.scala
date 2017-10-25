// ****************************
// 3.4 Working with Data - S3
// ****************************

/*
 For those working in the cloud, you may have a lot of your data 
 stored in an object store, like S3, and not wish to move it. 
 Luckily, Spark has native integration to s3 as well. 
 You can have spark read from s3 into a Dataset and save to s3 as well, 
 all the while utilizing the spark cluster for compute power. 

 In order to do this, we need to provide spark with our access key and 
 secret key. This allows it to communicate with S3 on our behalf. You can do so
 with the following commands

 sc.hadoopConfiguration.set("fs.s3a.access.key", "<your_access_key>")
 sc.hadoopConfiguration.set("fs.s3a.secret.key", "<your_secret_key>")

 CSDW users can also utilize environment variables to access S3. You can
 specify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your project's engine's 
 environment variables, and spark establish the connection automatically. 

![Settings](./images/settings.png)
 As an example, I have MLK Jr.'s "I Have a Dream" speech in a s3 bucket. 
 Let's load it into Spark.
 
 Note: For this example, you'll need access to an S3 bucket and upload
 the file in /data/ihaveadream/ihaveadream.txt into a diretory there. You 
 can then modify the diretories below as needed. 
 */

val dreams = spark.read.textFile("s3a://jvolz/ihaveadream.txt")

dreams.take(10).foreach(println)
  
// Let's calculate word counts and save to s3
  
val counts = dreams.
  flatMap(line => line.split(" ")).
  map(word => (word, 1)).
  rdd.reduceByKey(_ + _)

val r = scala.util.Random
val outputDir="s3a://jvolz/output/" + r.nextInt(1000000).toString
  
counts.saveAsTextFile(outputDir)
  
// Let's check our results
  
val countsS3 = spark.read.textFile(outputDir + "/*")
  
countsS3.head(25)