# 运行方式
## 克隆项目
```
git clone https://github.com/Raopend/movies_analysis.git
```
## 安装 sbt
sbt 官网安装指南：https://www.scala-sbt.org/download.html
## 修改数据库配置文件
修改 `src/main/resources/application.properties` 文件中的数据库配置信息：
```
# 数据库要提前建立，这里是 movies
database.url=jdbc:mysql://localhost:3306/movies 
database.user=root
database.password=123456
```
## 定义数据模型
根据需求在 `src/main/scala/models`下定义数据模型，模型直接映射了数据库里面的表，也就是说不需要在数据建表：
```
tenGreatestMoviesByAverageRating.scala
package models

case class tenGreatestMoviesByAverageRating(
   movieId: String, // 电影的id
   title: String, // 电影的标题
   avgRating: String // 电影平均评分
)
```
## 补充需求
在在 `src/main/scala/applications/Main.scala`文件里补充代码，应当定义为一个函数，依次来就行，
以需求一举例：
```scala
object Main {
    // 文件路径, 这里展示 csv, dat 文件有特定的读取方式，见例子
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
        // 这里是 Dataset 方式，对应的 RDD, DataFrame 直接调用 insertDataRDD 和 insertDataDataFrame 就行，数据库插入代码在 util/DatabaseAccess.scala下，一般不需要特殊修改
    }
    
    def main(args: Array[String]): Unit = {
        test1()
    }
}
```
## 运行
```
sbt run
```