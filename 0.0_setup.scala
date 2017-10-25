// ****************************
// 0.0 - Setup
// ****************************


import sys.process._

// create new diretories
"hadoop fs -mkdir /tmp/BasketballStats_csv"!
"hadoop fs -mkdir /tmp/BasketballStats_pq"!
"hadoop fs -mkdir /tmp/TelcoChurn"!
"hadoop fs -mkdir /tmp/Friends"!
"hadoop fs -mkdir /tmp/delete_me"!
"hadoop fs -mkdir /tmp/delete_me/basketball"!
"hadoop fs -mkdir /tmp/ssc"!
"hadoop fs -mkdir /tmp/ml/"!


// copy files to HDFS
val status = Seq("/bin/sh", "-c", "hadoop fs -put ./data/basketball_csv/* /tmp/BasketballStats_csv").!
val status = Seq("/bin/sh", "-c", "hadoop fs -put ./data/basketball_parquet/* /tmp/BasketballStats_pq").!
val status = Seq("/bin/sh", "-c", "hadoop fs -put ./data/telco_churn/* /tmp/TelcoChurn").!
val status = Seq("/bin/sh", "-c", "hadoop fs -put ./data/friends/* /tmp/Friends").!

// create Impala Tables
val IMPALA_HOST="cdsw-demo-1.vpc.cloudera.com"
val QUERY_STRING = "CREATE DATABASE IF NOT EXISTS basketball"
val IMPALA_STRING = s"impala-shell -i $IMPALA_HOST -q '$QUERY_STRING'" 
val status = Seq("/bin/sh", "-c", s"$IMPALA_STRING").!

val QUERY_STRING = """CREATE EXTERNAL TABLE IF NOT EXISTS basketball.players LIKE PARQUET "/tmp/BasketballStats_pq/part-00000-f2da7cd3-21d5-4e4c-84b1-a5b7840031f4.snappy.parquet" STORED AS PARQUET LOCATION "/tmp/BasketballStats_pq/" """
val IMPALA_STRING = s"impala-shell -i $IMPALA_HOST -q '$QUERY_STRING'" 
val status = Seq("/bin/sh", "-c", s"$IMPALA_STRING").!
