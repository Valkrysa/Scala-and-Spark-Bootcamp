import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

var spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("Clean-USA-Housing.csv")

data.printSchema

// need to import machine learning stuff and get data into format ("label", "features")
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Price").as("label"),
    $"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms", $"Area Population"
))

val assembler = (new VectorAssembler()
    .setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population"))
    .setOutputCol("features"))

val output = assembler.transform(df).select($"label", $"features")

val lr = new LinearRegression()

val lrModel = lr.fit(output)

val trainingSummary = lrModel.summary

trainingSummary.residuals.show()
trainingSummary.predictions.show()
trainingSummary.r2 // how much variance is explained in our model, 91% is not so bad
