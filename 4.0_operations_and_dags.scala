
// ****************************
// 4.0 Spark Operations and DAGs
// ****************************

/*
 Now that we know how to work with some different storage layers, 
 we can begin exploring different ways in which we can interact with data in Spark.
 Spark has two types of operations, transformations and actions. 

 Actions are commands that are computed by spark at the time of their exeuction. 
 Generally, these corresponds to operations that require Spark to return a value 
 or data back to the shell or application. Some examples include: 
 show, count, collect, take, save*, reduce. 
 We've already seen several examples of actions, but let's just highlight a few here. 
*/

//Let's read our basketball data
val fileName="/tmp/BasketballStats_csv/*"
val bballTxt=spark.read.text(fileName)
  
bballTxt.count()
  
bballTxt.take(10).foreach(println)
  
/* 
 Notice that when you run an action, you'll likely see the console turn red. 
 That is because spark is processing the action in order to return the desired result. 
    
 Transformations, on the other hand are not computed by spark when executed. 
 A Transformation turns one dataset into another dataset, and they are evaluated 
 lazily, meaning that Spark will not actually do the execution until it needs to. 
 Transformations are typically computed when the next action is executed. 
 We haven't yet interacted much with transformations, but they are widespread in spark.
 Here's a small list of some transformations: map, filter, join, groupBy, select.
  
 Let's take a look at some transformations we can apply to our data set.
*/

// Let's filter the dataset to look at only Boston Celtics
val bballBOS=bballTxt.as[String].filter(line => line.toString.contains("BOS"))

/* 
 Now we'll just pull out 2 fields we're interested in, age, and pts. 
 We'll also put a 1 into each row, which will allow us to easily 
 take an average in the next step
 */
val bballBOS2=bballBOS.map{line => 
    val pieces=line.split(",")
    val age = pieces(3).toInt
    val points  = pieces(9).toDouble
    (age, (points,1))}  

// Now we want to group by age and find the average points
val bballBOS3 = bballBOS2.rdd.
  reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2)).
  mapValues(v => 1.0 * v._1/v._2).
  sortByKey()
  
bballBOS3.collect

/*
 Notice that nothing happens when we apply the map, reduceByKey, and mapValues functions,
 but we trigger an action with sortByKey and collect. 
  
 Spark's flexibility lies in its ability to perform lambda functions via the map function. 
 This gives the Spark user much freedom in how they work with data and the action they take on it. 
 It is often common to define your operations as functions to make it easier to read. Consider
 the following: 
*/

def bosToNYK(team:String) = {
  if (team == "BOS"){
     "NYK"
    }
  else{team}
}

val bballBOS2NYK=bballBOS.map{line => 
    val pieces=line.split(",")
    val team = pieces(4)
    val name  = pieces(1)
    (name, bosToNYK(team))} 

bballBOS2NYK.take(5).foreach(println)
  

/*
 Don't worry too much about the notation above. It helps to have basic knowledge of 
 functional programming when working with Spark. We'll see in later sections how Spark APIs 
 can make much of this easier with a few simple commands. 
  
 Strictly speaking, the above example is close to a pure RDD example. Working with RDDs provide a lot 
 of flexibility, but they can get confusing and unwieldy quickly. When working with Datasets, it's a good
 idea to try to always enforce a strict type on the data. This makes it much eaiser to work with and you 
 can avoid some of the more cumbersome operations. This won't always be possible, and in those cases you can
 always fall back to working with RDDs. 
  
 Let's look at an example. We'll again work with our friend data
*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
  
case class Friend(name: String, age: Int, city: String)  

val friendSchema = StructType(
   StructField("name", StringType , false) ::
   StructField("age" , IntegerType, true ) ::
   StructField("city", StringType , true ) :: Nil)

val fileName="/tmp/friends/myfriends.csv"  

val myFriends=spark.read.
    option("header", false).
    schema(friendSchema).
    csv(fileName).
    as[Friend]

val myFriends30=myFriends.filter(friend => friend.age < 30)

myFriends30.show
  
val myFriendsAvgAge=myFriends.agg(avg('age))
  
myFriendsAvgAge.collect

  
/*
 Now that we have a grasp on transformations and actions, we should talk about how spark processes
 your job internally. 
  
 Recall that when spark reads in data, it creates a RDD or Dataset. This is a spark object that
 is paritioned and distributed across the spark cluster. Even though this is an object that lives on
 many different nodes, Spark views it as a single object. Instead of modifying an existing RDD, Spark 
 performs transformations on an RDD that create a new RDD. In this way, Spark creates a lineage of your 
 data as it changes through a job. Spark creates a DAG (directed acylcic graph) that shows how all these
 transformations are mapped out for your job in the form of an execution plan. In the process, Spark 
 will optimize the process and merge together transformations, creating a set of *stages*. Each stage 
 consists of many *tasks*. A spark *job* will piece together many different stages. 
  
 Each task runs the same set code on a different partition of data. A spark job can consist of hundreds or 
 thousands of tasks. Stages group together tasks that are running the same code, and stages are generally
 broken up by actions that require transfer or shuffling of data, such as performing a join or a group by. 
  
 The nice thing about the way that Spark executes its jobs is that the lineage provides an easy way for 
 Spark to repair itself in the case of failure. When a task fails, Spark doesn't need to recompute
 the entire stage or job to retrieve the result. It can simply rerun the missing task to get the missing
 data. 

![Spark DAG](./images/spark_dag.png)

 The architecture of Spark can also be important to understand. Spark consists of two core components, 
 a *driver* and *executors*. The driver acts as the master process -- it sends the executors code to run and tells 
 it which data to access, etc. The executors do all the heavy lifting while performing the transformations and
 crunching the data. Executors send data back to the driver when a action is performed, like running a collect 
 command. When running Spark on CDH, Spark runs on YARN, which manages all the resources for Spark.
 Spark requests containers from YARN like any other YARN application, and the executors are spun up in YARN
 containers. 

![Spark Arch](./images/spark_arch.png)                                      

![Spark Arch](./images/spark_arch_2.png)

 Knowing about the components of a spark job and its architecture can really help achieve a better understanding
 of spark and help optimize your spark code. It is also very useful when debugging or troubleshooting issues
 with spark. 
*/  
  
