import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object User extends Enumeration {

  private val DELIMITER = "\\,"

  val ID, GENDER= Value

  val structType = StructType(
    Seq(
      StructField(ID.toString, IntegerType),
      StructField(GENDER.toString, StringType)
    )
  )

  def apply(row: String): User = {
    val array = row.split(DELIMITER, -1)
    User(
      array(ID.id).toInt,
      array(GENDER.id)
    )
  }
}
case class User(
                 id: Int,
                 gender: String
               )