// ****************************
// 2.0 - Getting Started with Spark
// ****************************

/*
Spark has two modes you can operate in, spark-shell and spark-submit.

 spark-shell is for interactive analysis and gives users a REPL to submit
 spark commands and interact with data in CDH

 spark-submit is for submitting spark applications to the cluster. 
 These are not interactive and need to be self-contained. Typically 
 spark-submit is used by compiling your code into a jar and running it
 on the cluster (or giving it a .py file for pyspark))

 CDSW is an interactive tool and makes use of spark-shell. The expectation 
 is that users want to interact with data in real time. This tutorial is 
 designed to run in CDSW and will closely mirror a typical spark-shell 
 experience.
*/