
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Rate extends Enumeration {

  private val DELIMITER = "\\,"

  val MOVIE_ID, USER_ID, RATE = Value

  val structType = StructType(
    Seq(
      StructField(MOVIE_ID.toString, IntegerType),
      StructField(USER_ID.toString, IntegerType),
      StructField(RATE.toString, IntegerType)
    )
  )
}

case class Rate(
                 movie_id: Int,
                 user_id: Int,
                 rate: Int
               )

