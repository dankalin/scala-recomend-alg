import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Parameters {
  val POPULATION_DATASET_PATH = ""
  val EXAMPLE_OUTPUT_PATH = "./output/"


  val path_rate = "./dataset/movielens/ratings/*"
  val path_movie = "./dataset/movielens/movies/*"
  val path_user = "./dataset/movielens/users/*"

  val table_rate = "rate"
  val table_movie = "movie"
  val table_user = "user"

  private def createTable(name: String, structType: StructType, path: String, delimiter: String = ",")
                         (implicit spark: SparkSession): Unit = {
    spark.read
      .format("com.databricks.spark.csv")
      //.option("inferSchema", "true")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N"
        )
      ).schema(structType).csv(path).createOrReplaceTempView(name)
  }

  def initTables(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_rate, Rate.structType, Parameters.path_rate)
    createTable(Parameters.table_movie, Movie.structType, Parameters.path_movie)
    createTable(Parameters.table_user, User.structType, Parameters.path_user)
  }

}
