import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

object SparkMovieRating {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Movie Rating").setMaster("local[*]")

    // Get the spark context with config settings from commandline
    val sc = new SparkContext(conf)

    // Get the SQL context from spark context
    val sqlContext = new SQLContext(sc)

    //Creating a Dataframe for movies.csv file
    val titlesDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    //Creating a Dataframe for reviews.csv file
    val reviewsDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))

    // Grouping, joining, filtering movies with average rating > 4 and number of ratings > 10
    val movieRatingDF = reviewsDF.groupBy("movieId")
      .agg(avg("rating").cast("Double").alias("avgRating"),count("movieId").alias("numRating"))
      .filter(col("avgRating") > 4 && col("numRating") > 10 )
      .join(titlesDF,reviewsDF.col("movieId").equalTo(titlesDF.col("movieId")))
      .selectExpr("title","avgRating","numRating")
      .sort("avgRating")

    // Writing to single output part file
    movieRatingDF.map(x => x.mkString("\t"))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(args(2))

  }

}