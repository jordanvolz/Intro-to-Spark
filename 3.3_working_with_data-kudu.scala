// ****************************
// 3.3 Working with Data - Kudu
// ****************************

/* 
 Kudu is another type of storage layer provided by Cloudera.
 Kudu is great for use cases requiring fast analytics on fast data. 
 Core spark code does not currently have integration with Kudu, 
 so we need to add a jar to our SparkSession in order to work with Kudu. 

 When starting your spark-shell or running a spark-submit job, 
 you can add jars with the --jars switch or use --packages to pull
 it off of Maven.  

 For CDSW users, you can specify spark.jars or spark.jars.packages in a
 spark-default.conf file in the root of your project and CDSW
 will automatically pull this into your SparkSession for you. 
 This makes it easy to specifiy all the configuraitons you need for 
 your project. In this case, we have our kudu jar in the jar/ folder in our 
 project and we included the following in spark-defaults.conf

 spark.jars jars/kudu-spark2_2.11-1.1.0.jar

*/


/*
Once we have the proper jar loaded, we just import the library and start working with kudu.
*/

import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._

/*
 spark-kudu  has a KuduContext object that can be used to interact with kudu.
 This is useful for doing things like creating tables, inserting rows, etc. 
 We'll create a table here using a KuduContext
*/

val kuduMaster = "cdsw-demo-4.vpc.cloudera.com:7051"
val kuduContext = new KuduContext(kuduMaster)

// Define properties of the table, like name, schema, key, replicas, etc
  
val kuduTable = "my_friends_kudu"
 
val kuduTableSchema = StructType(
   StructField("name", StringType , false) ::
   StructField("age" , IntegerType, true ) ::
   StructField("city", StringType , true ) :: Nil)
 
val kuduPrimaryKey = Seq("name")
  
val kuduTableOptions = new CreateTableOptions()
kuduTableOptions.
 setRangePartitionColumns(List("name").asJava).
 setNumReplicas(3)
 
// Now call the createTable command
  
kuduContext.createTable(
 kuduTable, kuduTableSchema, 
 kuduPrimaryKey, kuduTableOptions)


/*
Another way to interact with data in Kudu is to read it directly
into a Dataframe using a SparkSession
*/
  

val kuduOptions= Map("kudu.master"-> kuduMaster,
                     "kudu.table"-> kuduTable)

val myFriendsKudu = spark.read.
  options(kuduOptions).
  kudu

// Now it's loaded into spark and we can do our normal spark actions on it. 
  
myFriendsKudu
  
myFriendsKudu.show()
  
myFriendsKudu.count()


/*
 Writing to kudu is similarly easy. You can either use the KuduContext, 
 which has apis for inserting rows, etc. Or you can just directly
 save from the Dataframe. Here, we'll do the latter  

 Load up some data that matches the table we just created. 
 Note that we give it the kudu table schema when we load it so
 it's already ready to go
*/

val fileName="/tmp/Friends/myfriends.csv"
  
val myFriends=spark.read.
  option("header", false).
  option("inferSchema", true).
  schema(kuduTableSchema).
  csv(fileName)
  
myFriends.show()
  
val kuduOptions= Map("kudu.master"-> kuduMaster,
                     "kudu.table"-> kuduTable)
  
myFriends.write.
  options(kuduOptions).
  mode("Append").
  kudu
  
// Check to see if the table now has data. 
  
val newFriends = spark.read.options(kuduOptions).kudu

newFriends.count()
  
newFriends.show()


// Let's clean up the table now that we're done
  
kuduContext.deleteTable(kuduTable)
  
  


  