package demos
import metrics._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
object DemoMainApp {
    // 文件路径
    private val MOVIES_CSV_FILE_PATH = "src/main/resources/movies.csv"
    private val RATINGS_CSV_FILE_PATH = "src/main/resources/movies.csv"


    def main(args: Array[String]): Unit = {
        // 创建spark session
        val spark = SparkSession
            .builder
            .master("local[*]")
            .getOrCreate
        // schema信息
        val schemaLoader = new SchemaLoader
        // 读取Movie数据集
        val movieDF = readCsvIntoDataSet(spark, MOVIES_CSV_FILE_PATH, schemaLoader.getMovieSchema)
        // 输出前10条数据
        movieDF.show(10)
        // 读取Rating数据集
        val ratingDF = readCsvIntoDataSet(spark, RATINGS_CSV_FILE_PATH, schemaLoader.getRatingSchema)
        movieDF.printSchema()
        ratingDF.printSchema()

        // 需求1：查找电影评分个数超过5000,且平均评分较高的前十部电影名称及其对应的平均评分
        val bestFilmsByOverallRating = new BestFilmsByOverallRating
        bestFilmsByOverallRating.run(movieDF, ratingDF, spark)

        // 需求2：查找每个电影类别及其对应的平均评分
        val genresByAverageRating = new GenresByAverageRating
        genresByAverageRating.run(movieDF, ratingDF, spark)

        // 需求3：查找被评分次数较多的前十部电影
        val mostRatedFilms = new MostRatedFilms
        mostRatedFilms.run(movieDF, ratingDF, spark)


        spark.close()

        /**
         * 读取数据文件，转成DataFrame
         *
         * @param spark
         * @param path
         * @param schema
         * @return
         */

    }

    private def readCsvIntoDataSet(spark: SparkSession, path: String, schema: StructType) = {
        val dataSet = spark.read
            .option("header", "true")
            .schema(schema)
            .csv(path)
        dataSet
    }
}