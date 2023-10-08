package applications

import models._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.{AppConfig, DatabaseAccess}

import java.io.FileInputStream
import java.util.Properties

object Main {
    // 文件路径
    private val MOVIES_CSV_FILE_PATH = "data/movies.csv"
    private val RATINGS_CSV_FILE_PATH = "data/ratings.csv"
    val spark = SparkSession
        .builder
        .master("local[*]")
        .getOrCreate
    val schemaLoader = new SchemaLoader
    // 数据库
    val config: AppConfig = loadConfigFromResource()
    val dbAccess = new DatabaseAccess(config)
    // 读取Movie数据集
    val movieDF: DataFrame = readCsvIntoDataSet(spark, MOVIES_CSV_FILE_PATH, schemaLoader.getMovieSchema)
    // 读取Rating数据集
    val ratingDF: DataFrame = readCsvIntoDataSet(spark, RATINGS_CSV_FILE_PATH, schemaLoader.getRatingSchema)

    import spark.implicits._
    // 将moviesDataset注册成表
    movieDF.createOrReplaceTempView("movies")
    // 将ratingsDataset注册成表
    ratingDF.createOrReplaceTempView("ratings")

    def test1(): Unit = {
        /*
        需求1：查找电影评分个数超过5000,且平均评分较高的前十部电影名称及其对应的平均评分
         */
        // 查询SQL语句
        val sql =
        """
          |WITH ratings_filter_cnt AS (
          |SELECT
          |	    movieId,
          |	    count( * ) AS rating_cnt,
          |	    avg( rating ) AS avg_rating
          |FROM
          |	    ratings
          |GROUP BY
          |	    movieId
          |HAVING
          |	    count( * ) >= 5000
          |),
          |ratings_filter_score AS (
          |SELECT
          |     movieId, -- 电影id
          |     avg_rating -- 电影平均评分
          |FROM ratings_filter_cnt
          |ORDER BY avg_rating DESC -- 平均评分降序排序
          |LIMIT 10 -- 平均分较高的前十部电影
          |)
          |SELECT
          |	   m.movieId,
          |	   m.title,
          |	   r.avg_rating AS avgRating
          |FROM
          |	  ratings_filter_score r
          |JOIN movies m ON m.movieId = r.movieId
        """.stripMargin

        val resultDS = spark.sql(sql).as[tenGreatestMoviesByAverageRating]
        dbAccess.insertDataDataset(resultDS, "tenGreatestMoviesByAverageRating")
    }

    def test2(): Unit = {
        // 需求2：查找每个电影类别及其对应的平均评分
        val sql =
            """
              |WITH explode_movies AS (
              |SELECT
              |	movieId,
              |	title,
              |	category
              |FROM
              |	movies lateral VIEW explode ( split ( genres, "\\|" ) ) temp AS category
              |)
              |SELECT
              |	m.category AS genres,
              |	avg( r.rating ) AS avgRating
              |FROM
              |	explode_movies m
              |	JOIN ratings r ON m.movieId = r.movieId
              |GROUP BY
              |	m.category
              | """.stripMargin

        val resultDS = spark.sql(sql).as[topGenresByAverageRating]
        dbAccess.insertDataDataset(resultDS, "topGenresByAverageRating")
    }

    def test3(): Unit = {
        // 需求3:被评分次数较多的前十部电影
        val sql =
            """
              |WITH rating_group AS (
              |    SELECT
              |       movieId,
              |       count( * ) AS ratingCnt
              |    FROM ratings
              |    GROUP BY movieId
              |),
              |rating_filter AS (
              |    SELECT
              |       movieId,
              |       ratingCnt
              |    FROM rating_group
              |    ORDER BY ratingCnt DESC
              |    LIMIT 10
              |)
              |SELECT
              |    m.movieId,
              |    m.title,
              |    r.ratingCnt
              |FROM
              |    rating_filter r
              |JOIN movies m ON r.movieId = m.movieId
              |
          """.stripMargin

        val resultDS = spark.sql(sql).as[tenMostRatedFilms]
        dbAccess.insertDataDataset(resultDS, "tenMostRatedFilms")
    }

    def test4(): Unit = {

    }

    def main(args: Array[String]): Unit = {
        test1()
        test2()
        test3()
        dbAccess.close()
    }

    // 从配置文件加载配置
    def loadConfigFromResource(): AppConfig = {
        val properties = new Properties()
        val configFile = "src/main/resources/application.properties"
        properties.load(new FileInputStream(configFile))
        AppConfig(
            properties.getProperty("database.url"),
            properties.getProperty("database.user"),
            properties.getProperty("database.password")
        )
    }

    // 加载 dataset
    def readCsvIntoDataSet(spark: SparkSession, path: String, schema: StructType): DataFrame = {

                                val dataSet = spark.read
            .format("csv")
            .option("header", "true")
            .schema(schema)
            .load(path)
        dataSet
    }
}
