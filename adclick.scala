
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)


val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("advertising.csv")


data.printSchema()
data.show()

val colnames = data.columns
val firstrow = data.head(1)(0)
println("\n")
println("Sample row")
for(ind <- Range(1,colnames.length)){
  println(colnames(ind))
  println(firstrow(ind))
  println("\n")
}

val timedata = data.withColumn("Hour",hour(data("Timestamp")))

val logregdata = (timedata.select(data("Clicked on Ad").as("label"),
                $"Daily Time Spent on Site", $"Age", $"Area Income",
                $"Daily Internet Usage",$"Male"))
