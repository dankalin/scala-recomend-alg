import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, length}
import java.io._
import scala.util.Try

object Main extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]").appName("untitled")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("WARN")
  Logger.getLogger("org").setLevel(Level.OFF)

  import spark.implicits._

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  Parameters.initTables(spark)

  val ratingsDF = spark.table("rate")

  val moviesDF = spark.table("movie")

  val usersDF = spark.table("user")

  val test_1_1 = hasColumn(ratingsDF, "movie_id") && hasColumn(ratingsDF, "user_id") && hasColumn(ratingsDF, "rate")
  val test_2_1 = hasColumn(moviesDF, "movie_id") && hasColumn(moviesDF, "name") && hasColumn(moviesDF, "genre")
  val test_3_1 = hasColumn(usersDF, "id") && hasColumn(usersDF, "gender")

  if (test_1_1 && test_2_1 && test_3_1) {

    val joinMoviesRatingsDF = moviesDF.join(ratingsDF,
      moviesDF("movie_id") === ratingsDF("movie_id"), "inner")
      .select($"movie.movie_id", $"user_id", $"rate")                                                              //таблица с фильмами и рейтингами пользователей
      .orderBy("user_id")

    val test_1_2 = hasColumn(joinMoviesRatingsDF, "movie_id") && hasColumn(joinMoviesRatingsDF, "user_id") && hasColumn(joinMoviesRatingsDF, "rate")

    if (test_1_2) {

      println("Введите id пользователя для которого хотите подобрать фильмы:")
      val user_id = scala.io.StdIn.readLine()                                                                            //id пользователя которому мы хотим подобрать фильм

      if (!(joinMoviesRatingsDF
        .select($"movie_id")
        .filter($"user_id" === user_id).isEmpty)) {

        val userCurrentFilmsGood = joinMoviesRatingsDF                                                                   //хорошо оценненые фильмы нашего юзера
          .select($"movie_id")
          .filter($"user_id" === user_id && $"rate".isin(4, 5))
          .orderBy("movie_id")

        val test_1_4 = hasColumn(userCurrentFilmsGood, "movie_id")

        if (test_1_4) {

          val sovpadenia = joinMoviesRatingsDF.join(userCurrentFilmsGood,                                               //юзер у которых больше всего совпадений хороших фильмов с нашим юзером
            joinMoviesRatingsDF("movie_id") === userCurrentFilmsGood("movie_id") &&
              joinMoviesRatingsDF("rate") >= 4 && $"user_id" =!= user_id, "inner")
            .groupBy("user_id").count().orderBy(desc("count")).limit(1)

          val test_1_5 = hasColumn(sovpadenia, "user_id") && hasColumn(sovpadenia, "count")

          if (test_1_5) {

            val res = joinMoviesRatingsDF.join(sovpadenia,                                                              //выводим все фильмы юзера с которым больше всего совпадений
              joinMoviesRatingsDF("user_id") === sovpadenia("user_id"),
              "inner").filter($"rate" >= 4)
              .select($"movie_id")

            val test_1_6 = hasColumn(res, "movie_id")

            if (test_1_6) {
              val resultFilms = res.join(userCurrentFilmsGood,                                                          //id фильмов которых не было у нашего юзера,но есть у того с кем больше всего совпадений
                res("movie_id") === userCurrentFilmsGood("movie_id"), "leftanti")

              val test_1_7 = hasColumn(resultFilms, "movie_id")

              if (test_1_7) {
                val resultFilmsUser = resultFilms.join(moviesDF,                                                        //какие фильмы надо показывать нашему юзеру
                  resultFilms("movie_id") === moviesDF("movie_id"), "inner")
                  .select($"name")

                val resultGenreUser = resultFilms.join(moviesDF,                                                        //какие фильмы надо показывать нашему юзеру
                  resultFilms("movie_id") === moviesDF("movie_id"), "inner")
                  .select($"genre")

                val test_1_8 = hasColumn(resultFilmsUser, "name")
                val test_2_8 = hasColumn(resultGenreUser, "genre")

                if (test_1_8 && test_2_8) {
                  print("Рекомендованные фильмы: "
                    + resultFilmsUser.collect.mkString.replaceAll("[\\[\\]]", ",").replaceAll(",,", ", ").dropRight(1).drop(1)
                  )
                  val data = Array("Рекомендованные фильмы: "
                    + resultFilmsUser.collect.mkString.replaceAll("[\\[\\]]", ",").replaceAll(",,", ", ").dropRight(1).drop(1))
                  printToFile(new File("result.txt")) { p =>
                    data.foreach(p.println)
                  }
                }
                else print("Ошибка 8 теста")
              }
              else print("Ошибка 7 теста")
            }
            else print("Ошибка 6 теста")
          }
          else print("Ошибка 5 теста")
        }
        else print("Ошибка 4 теста")
      }
      else
        print("Такого юзера не существует,ошибка в 3 тесте")
    }
    else
      print("Ошибка 2 теста")
  }
  else
    print("Ошибка 1 теста")
}
