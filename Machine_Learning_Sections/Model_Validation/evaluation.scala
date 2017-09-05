import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// read in data
val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("../Regression/Clean-USA-Housing.csv")
data.printSchema

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = data.select(data("Price").as("label"), $"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms", $"Area Population")

val assembler = new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")).setOutputCol("features")

val output = assembler.transform(df).select($"label", $"features")

// build training and test data
val Array(training, test) = output.select("label", "features").randomSplit(Array(0.7, 0.3), seed = 12345)

// linear regression model
val lr = new LinearRegression()

// build parameter grid
// we could also just leave addGrid off to ignore the parameter grid effect
val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(10000, 0.1)).build()

// train split (holdout group)
val trainValSplit = (
    new TrainValidationSplit()
    .setEstimator(lr)
    .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
    .setEstimatorParamMaps(paramGrid)
    .setTrainRatio(0.8)
)

val model = trainValSplit.fit(training)

model.transform(test).select("features", "label", "prediction").show()

model.validationMetrics // r2 selected above but root mean squared error is default if no setMetricName is set

// 92.7% model fit according to r2
