## 2.1 - Getting Started with Pyspark
# (Note: Run this in a python session!)

## Creating the SparkSession (pyspark)
# The SparkSession is the object used to communicate with spark. 
# Python and R users must explictly create a SparkSession object, whereas scala users do not. 

# To do so, we import SparkSession from pyspark.sql and use the 
# SparkSession.builder.getOrCreate() command, optionally adding configurtion to the 
# Spark Session in the process
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .appName("Intro to Spark") \
      .getOrCreate()

# Let's verify that we have a spark object
    
spark

# Spark 1.x contains two contexts, a sparkContext and a sqlContext. 
# In Spark 2.x, they were combined into a SparkSession object, but there is still useful information
# under spark.sparkContext, such as the applicationID. The reason for the change to the SparkSession 
# has to do with enclosing RDDs and Dataframes into a common Dataset object, which we'll explore in an upcoming section.

spark.sparkContext.applicationId

# The applicationId is useful to connect it back to the spark History server in case you need to troubleshoot the job, etc. 
# Take the ID and open the SparkHistory Server in the top right corner of CDSW to look for your spark job. 

# Note that when a SparkSession is present, CDSW will automatically link directly to the job in the UI itself.
# Right now we haven't done anything, so there's nothing interesting in the Spark UI. 

# It's always good practice to shut down your SparkSession when you are finished. 

spark.stop()

  
# Note, when creating your SparkSession, you can pass in configuration values via
# .config("<config.option>","value")
# We'll talk more about configurations in an upcoming section