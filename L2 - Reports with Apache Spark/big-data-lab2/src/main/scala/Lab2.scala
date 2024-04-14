import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Lab2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lab1")
      .master("local[*]")
      .getOrCreate()

    val programmingLanguagesDataFrame: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/programming-languages.csv")

    val programmingLanguages = programmingLanguagesDataFrame.select("name")
      .rdd
      .map(lang => lang(0))
      .map(lang => lang.toString)
      .map(lang => lang.toLowerCase)
      .collect()
      .toList

    programmingLanguages.take(10).foreach(println)

    val posts = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .option("inferSchema", value = true)
      .load("data/posts_sample.xml")

    // Пример содержимого yearLanguageTags [(2009, java), (2011, c#)]
    val yearLanguageTags = posts
      .rdd
      .map(row => (row(6), row(18))) // (CreationDate, Tags)
      .filter(row => row._1 != null & row._2 != null)
      .map(row => (row._1.toString, row._2.toString))
      .map(row => (row._1.substring(0, 4), row._2.substring(1, row._2.length - 1).split("><"))) // row._1.substring(0, 4) - год
      .flatMap {
        case (year, tags) => tags.map(tag => (year, tag))
      }
      .filter(row => programmingLanguages.contains(row._2))

    val years = (2010 to 2020).map(_.toString)

    val finalReport = years.flatMap(year =>
      yearLanguageTags
        .filter(row => row._1 == year)
        .map(row => (row._2, 1))
        .reduceByKey(_ + _)
        .map(row => (year, row._1, row._2)) // [(2010, java, 15), ..]
        .sortBy(row => row._3, ascending = false)
        .take(10)
        .toList
    )

    val df = spark.createDataFrame(finalReport).select(
      col("_1").alias("Year"),
      col("_2").alias("Language"),
      col("_3").alias("Count")
    )

    df.show()

    df.write
      //.mode("overwrite")
      .parquet("report")

    spark.stop()
  }
}