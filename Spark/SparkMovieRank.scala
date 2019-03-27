import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext

object SparkMovieRank {

  // Takes movies.csv and reviews.csv files and creates an output file with the movie titles, counts 
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Movie Rank").setMaster("local[*]")

    // Set the spark context with config settings from commandline
    val sc = new SparkContext(conf)

    // setup SQL context
    val sqlContext = new SQLContext(sc)

    // Creating a Dataframe for movies.csv file
    val titlesDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    // Creating a Dataframe for reviews.csv file
    val reviewsDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))

    // Aggregating the counts for each movieId, Joining the 2 dataframes based on movieId and writing the output in the sorted order
    val movieRankDF = reviewsDF.groupBy("movieId").count()
        .join(titlesDF, reviewsDF.col("movieId").equalTo(titlesDF.col("movieId")))
        .selectExpr("count","title")
        .sort("count")

    // Write to only one part file in the output
    movieRankDF.map(x => x.mkString("\t"))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(args(2))

  }

}