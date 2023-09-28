package demos

import org.apache.spark.sql.types._

class SchemaLoader {
    private val movieSchema = StructType(Seq(
        StructField("movieId", StringType),
        StructField("title", StringType),
        StructField("genres", DoubleType)
    ))

    private val ratingSchema = StructType(Seq(
        StructField("userId", StringType),
        StructField("movieId", StringType),
        StructField("rating", DoubleType),
        StructField("timestamp", StringType)
    ))

    def getMovieSchema: StructType = movieSchema

    def getRatingSchema: StructType = ratingSchema
}
