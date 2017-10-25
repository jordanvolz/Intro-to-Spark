// ****************************
// 7.0 Spark ML/MLLib
// ****************************

/*
 The final API we'll take a look at in Spark is the Machine Learning library. 
 Like Spark Streaming and Structured Streaming, Spark has two packages, one for 
 RDDs, known as MLLib, and one for Dataframes/Datasets, known as ML. As the emphasis
 moving forward in the community is on the ML packages, we'll also focus on that. The
 parity between the two packages is pretty good currently, although most future development
 will likely be incresingly limited to ML, as MLLIB is being deprecated. 

 Spark ML provides many different tools: basic statistics, distributed algorithms,
 featurization, a pipeline api, and model persistence. Many know of Spark ML for its 
 distributed algorithms, but it also has nice tools for building pipelines, which makes it 
 easy to iterate on models. The distributed algorithms span many different aspects of machine learning, 
 like classification, clustering, regression, collaborative filtering, pattern matching, etc. 
 To begin working with Spark ML, it's important to understand a few basic concepts about it. 

 Since we are working with an API built on Dataframes, it's not surprising that Dataframes are the core
 object in Spark ML. Spark ML introduces the notion of *transformers*, *estimators*, and *pipelines*.

 A Transfomer changes a Dataframe into another Dataframe. For example, a machine learning model is a 
 transformer because it transforms a dataframe with features into a dataframe with predicitons. A Transfer
 cloud also transform a column in a Dataframe into a Dataframe with a different column, such as indexing a certain
 feature. 

 Estimators change a Dataframe into a Transformer. A good example of an Estimator is a machine learning 
 algorithm that fits onto a dataset to produce a model. That model is then a Transformer because it can be used
 to make predictions. 

 A Pipeline is a chain of transfomers and estimators. Pipelines make it easy to change parts of 
 the DS workflow in order ot move parts around or change specific pieces without having to rewrite a lot of code. 
 You can also save and load pipelines in Spark. 

![Spark Pipeline](./images/pipelines_1.png)

![Spark Pipeline](./images/pipelines_2.png)

 We now know enough to look at a quick ML example. For this use case, we'll look at some telco customer data
 and create a model to predict customer churn. 
*/

// Import parts of spark.ml we'll be using
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline,PipelineModel}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._

  
// Read in data from csv file in HDFS and apply a schema to it. 
val fileName="/tmp/TelcoChurn/churn.all"
val churnSchema = StructType(Array(StructField("state", StringType, true),
        StructField("account_length", DoubleType, true),
        StructField("area_code", StringType, true),
        StructField("phone_number", StringType, true),
        StructField("intl_plan", StringType, true),
        StructField("voice_mail_plan", StringType, true),
        StructField("number_vmail_messages", DoubleType, true),     
        StructField("total_day_minutes", DoubleType, true),     
        StructField("total_day_calls", DoubleType, true),     
        StructField("total_day_charge", DoubleType, true),     
        StructField("total_eve_minutes", DoubleType, true),     
        StructField("total_eve_calls", DoubleType, true),     
        StructField("total_eve_charge", DoubleType, true),     
        StructField("total_night_minutes", DoubleType, true),     
        StructField("total_night_calls", DoubleType, true),     
        StructField("total_night_charge", DoubleType, true),     
        StructField("total_intl_minutes", DoubleType, true),     
        StructField("total_intl_calls", DoubleType, true),     
        StructField("total_intl_charge", DoubleType, true),     
        StructField("number_customer_service_calls", DoubleType, true),     
        StructField("churned", StringType, true)))
val churn_data=spark.read.
  option("header", false).
  schema(churnSchema).
  csv(fileName)

// Peek at the data set
churn_data.show()  


/*
 To use ML on our data set, we need to convert our String features into numerical features.
 We can do this with a StringIndexer Transformer. This takes a column and converts it to a 
 numerical index. We tell it to index the column 'churned' into the new column 'label'.
*/

val label_indexer = new StringIndexer().
  setInputCol("churned").
  setOutputCol("label")
  
val plan_indexer = new StringIndexer().
  setInputCol("intl_plan").
  setOutputCol("intl_plan_indexed")

/*
 We run another Transformer on the Dataframe. This time we create a 'features' column that contains
 all the data we want our model to consider.
*/
val reduced_numeric_cols = Array("account_length", "number_vmail_messages", "total_day_calls",
                        "total_day_charge", "total_eve_calls", "total_eve_charge",
                        "total_night_calls", "total_night_charge", "total_intl_calls", 
                        "total_intl_charge","number_customer_service_calls", "intl_plan_indexed")

val assembler = new VectorAssembler().
    setInputCols(reduced_numeric_cols).
    setOutputCol("features")
  
/*
 We pick our classification algorithm. We have to tell the algorithm what column corresponds to the label
 and features. Depending on the algorithm, we may also have to specify other parameters. For a random forest, 
 we instruct it to create 10 trees. This is an estimator, as it will create a model. 
*/

val classifier = new RandomForestClassifier().
  setLabelCol("label"). 
  setFeaturesCol("features").
  setNumTrees(10)

/*
 This is our pipeline! All we have to do is tell spark the order in which we want to apply our 
 Pipeline Stages. Here we index two columns, assemble a feature vector, and run a classification
 algorithm on our data, which will produce a model. 
*/
val pipeline = new Pipeline().
  setStages(Array(plan_indexer, label_indexer, assembler, classifier))

// Create a training and testing Dataframe from our data.     
val split = churn_data.randomSplit(Array(0.7, 0.3))
val train = split(0)
val test = split(1)

// Fit the training data into the pipeline to create our model.   
val model = pipeline.fit(train)

// Run our test data through the model to get our predictions. 
val predictions = model.transform(test)

/*
 Evaluate the predictions. We can use our classificaiton evaluator in order to do this. We 
 specify the metric to test and the column that corresponds to the label. Then we run the 
 evaluate function on the predicitons. 
*/
val evaluator = new BinaryClassificationEvaluator().
  setMetricName("areaUnderROC").
  setLabelCol("label")

val auroc = evaluator.evaluate(predictions)

print ("The AUROC is " + auroc)
  
// If your evaluation was good, you may be intersted in actually checking out the predictions!
predictions.select($"label",$"prediction").show(25)
  
  
/*
 Spark contains some tools to help with model tuning as well. The one that is 
 likely of most interest is hyperparameter tuning. Using a ParamGrindBuilder,
 you can specify many parameters for Spark to test at the same time. There are currently
 two ways to test them, cross validation, and train validation. 
  
 Cross validation breaks the dataset into k "folds", and runs each set of parameters on 
 every fold, taking an average of the evaluator to get your final result. Train validation
 only tests each set of parameters once, instead of k times. Cross validation is more expensive
 to compute, but generally yields much better results. When running on a cluster when you can take 
 advantage of parallelism, cross validation is recommended. 
  
 We'll show an example of cross validation below. 
*/  
  
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, CrossValidatorModel}

/*
 Build the parameter grid. We'll try 3 different values of 3 parameters, which will be
 27 models to test in total!
*/
val paramGrid = new ParamGridBuilder().
  addGrid(classifier.maxBins, Array(3, 5,10)).
  addGrid(classifier.maxDepth, Array(3, 5,10)).
  addGrid(classifier.numTrees, Array(5, 10, 20)).
  build()

/*
 Create the crossValidator. We need to give it the pipeline, evaluator, and param grid, 
 as well as the number of folds. We'll just reuse our data from above for simplicity.
*/
val cv = new CrossValidator().
  setEstimator(pipeline).
  setEvaluator(evaluator).
  setEstimatorParamMaps(paramGrid).
  setNumFolds(3)  

/*
 Now, just fit the training data onto cross validator to get your model. The 
 cross validator will return the best model.   
*/
val cvModel = cv.fit(train)

/*
 There are many ways we can interact with the cross validator. You might be interested to know 
 how the different combinations performed, or what's the best model.  
*/

cvModel.bestModel
  
// show the param map
cvModel.getEstimatorParamMaps

// show the metric sfrom the evaluator  
cvModel.avgMetrics

// match params to their evaluator
cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

// get params of the best model  
cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)
  
// Of course, you may want to see the predictions
val cvPredictions = cvModel.transform(test)
  

/*
 The last thing we'll look at is Spark's ability to save models/pipelines.
 This can be convenient when you want to move a model across environments or 
 simply don't want to recompute an expensive model. For example, I can compute a model
 in batch, save it, and then load it into a streaming job to score data as it 
 comes into the cluster. It's farily easy to execute this, as shown below. 
*/ 

// save model
model.write.overwrite.save("/tmp/ml")
  
// load model
val loadedModel = PipelineModel.read.load("/tmp/ml")

// verify model stages  
loadedModel.stages
  
val newPredictions = loadedModel.transform(test)
  
newPredictions.select($"label",$"prediction").show(25)
