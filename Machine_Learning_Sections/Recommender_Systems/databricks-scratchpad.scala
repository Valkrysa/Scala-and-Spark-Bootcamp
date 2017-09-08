import org.apache.spark.ml.recommendation.ALS
val data = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/0hbiilvh1504826516797/movie_ratings.csv")
data.printSchema
data.head(1)
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
val model = als.fit(training)
val predictions = model.transform(test)
import org.apache.spark.sql.functions._
val error = predictions.select(abs($"rating" - $"prediction"))
error.na.drop().describe().show()
predictions.show()
