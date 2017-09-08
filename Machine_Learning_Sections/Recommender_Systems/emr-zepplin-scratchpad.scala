// load from s3 instead of local
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3://alice-emr-test/movie_ratings.csv")
