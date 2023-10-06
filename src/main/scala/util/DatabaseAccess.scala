package util
import org.apache.spark.rdd.RDD
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.duration.Duration
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.Await

case class AppConfig(databaseUrl: String, databaseUser: String, databasePassword: String)

class DatabaseAccess(config: AppConfig) {
  val db = Database.forURL(config.databaseUrl, config.databaseUser, config.databasePassword, driver = "com.mysql.jdbc.Driver")
  val jdbcProperties = new java.util.Properties()
  jdbcProperties.setProperty("user", config.databaseUser)
  jdbcProperties.setProperty("password", config.databasePassword)
  // 插入数据
  def insertDataRDD[T](data: RDD[T], tableQuery: TableQuery[_ <: Table[T]])(implicit tt: scala.reflect.ClassTag[T]): Unit = {
    val batchInsertSize = 1000 // 设置批量插入大小
    data.foreachPartition { partition =>
      partition.grouped(batchInsertSize).foreach { batch =>
        val action = DBIO.seq(tableQuery ++= batch)
        Await.result(db.run(action), Duration.Inf)
      }
    }
  }

  // 通用插入方法，接受 DataFrame 作为输入
  def insertDataDataFrame(data: DataFrame, tableName: String): Unit = {
    data.write
        .mode("overwrite")
        .jdbc(config.databaseUrl, tableName, jdbcProperties)
  }

  // 通用插入方法，接受 Dataset 作为输入
  def insertDataDataset[T](data: Dataset[T], tableName: String): Unit = {
    data.write
        .mode("overwrite")
        .jdbc(config.databaseUrl, tableName, jdbcProperties)
  }

  // 关闭数据库连接
  def close(): Unit = db.close()
}
