import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Movie extends Enumeration {

  private val DELIMITER = "\\,"

  val MOVIE_ID, NAME, GENRE = Value

  val structType = StructType(
    Seq(
      StructField(MOVIE_ID.toString, IntegerType),
      StructField(NAME.toString, StringType),
      StructField(GENRE.toString, StringType)
    )
  )
}

case class Movie(
                  movie_id: Int,
                  name: String,
                  genre: String
                )
