import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")
// val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

df.printSchema()
df.show()

// Datetime operations
// val df2 = df.withColumn("Year", year(df("Date")))
// val dfavgs = df2.groupBy("Year").mean()
// dfavgs.select($"Year", $"avg(Close)").show()

// val dfmins = df2.groupBy("Year").min()
// dfmins.select($"Year", $"min(Close)").show()

// df.select(year(df("Date"))).show()
// df.select(month(df("Date"))).show()

// drop rows with null values
// df.na.drop().show()

// drop rows with a minimum number of non-null values
// df.na.drop(2).show()

// fill nulls of same type as passed value with passed value
// df.na.fill(100).show()
// df.na.fill("Missing Name").show()
// df.na.fill("New Name", Array("Name")).show()
// df.na.fill(200, Array("Sales")).show()

// To fill multiple save result and run filter again on saved result
// val df2 = df.na.fill(400.5, Array("Sales"))
// df2.na.fill("Missing Name", Array("Name")).show()

// default order is asc
// df.orderBy("Sales").show()

// to specify order use scala notation
// df.orderBy($"Sales".desc).show()

// group by and aggregate functions
// df.groupBy("Company").mean().show()
// df.groupBy("Company").count().show()
// df.groupBy("Company").max().show()
// df.groupBy("Company").min().show()
// df.groupBy("Company").sum().show()

// aggregate functions without group by
// df.select(sum("Sales")).show()
// df.select(countDistinct("Sales")).show() //approxCountDistinct
// df.select(sumDistinct("Sales")).show()
// df.select(variance("Sales")).show()
// df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
// df.select(collect_set("Sales")).show()

// val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")
// df.printSchema()
// df.columns

// lets you use scala notation instead of sql notation for accessing things by name
// import spark.implicits._

// check the correlation (Pearson Correlation Coefficient) for two columns.
//df.select(corr("High", "Low")).show()

// in sql notation equality is how you expect in sql
// df.filter("High = 484.40").show()

// you must use triple equals for filtering on equality in scala notation
// df.filter($"High" === 484.40).show()

// count rows
// val CH_low = df.filter($"Close" < 480 && $"High" < 480).count()

// collect values into an array
// val CH_low = df.filter($"Close" < 480 && $"High" < 480).collect()

// filter in scala notation
// df.filter($"Close" < 480 && $"High" < 480).show()

// filter in sql notation
// df.filter("Close < 480 AND High < 480").show()

// Get first 5
// for (row <- df.head(5)) {
//     println(row)
// }

// basic description and stats
// df.describe().show()

// query a field
// df.select("Volume").show()

// query multiple fields
// df.select($"Date", $"Close").show()

// calculated field and another way to do multi select
// val df2 = df.withColumn("HighPlusLow", df("High") + df("Low"))
// df2.printSchema()
// df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show()
