package applications

import applications.Main.loadConfigFromResource
import models.user
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import util.{AppConfig, DatabaseAccess};
object prepare_users {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .master("local[*]")
            .getOrCreate
        val usersRDD = spark.sparkContext.textFile("data/users.dat")
        val schemaString = "UserID Gender Age Occupation Zipcode"
        val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
        val rowRDD = usersRDD.map(_.split("::")).map(x ⇒ Row(x(0), x(1), x(2), x(3), x(4)))
        import spark.implicits._
        val users_1 = spark.createDataFrame(rowRDD, schema).as[user]
        val config: AppConfig = loadConfigFromResource()
        val dbAccess = new DatabaseAccess(config)
        dbAccess.insertDataDataset(users_1, "users")
    }
}
