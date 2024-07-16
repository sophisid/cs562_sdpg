// LSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, DataTypes}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable

object LSH {

    def setupLSH(data: DataFrame): BucketedRandomProjectionLSHModel = {
    // val bucketLength = averageVariance * 0.5 // Example dynamic calculation
    // val numHashTables = (10 / averageVariance).toInt.max(1) // Ensure at least one hash table
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(10.0)
      .setNumHashTables(10)
      .setInputCol("normFeatures")
      .setOutputCol("hashes")

    brp.fit(data)
  }

 def prepareDataForLSH(allPatterns: mutable.Map[Seq[String], mutable.ListBuffer[Seq[Any]]], spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Flatten the map into a sequence and convert to DataFrame
    val data = allPatterns.flatMap { case (pattern, listBuffer) =>
      listBuffer.zipWithIndex.map { case (row, index) =>
        val featureArray = row.map {
          case num: Number => num.doubleValue()
          case other => other.hashCode.toDouble
        }.toArray
        (pattern.mkString("|"), index, featureArray)
      }
    }.toSeq.toDF("pattern", "id", "array")

    // Find the maximum length of any vector
    val maxLength = data.select(max(size($"array"))).as[Int].collect()(0)

    // UDF to pad vectors to the maximum length
    val toPaddedVector = udf((array: Seq[Double]) => {
      Vectors.dense((array ++ Array.fill(maxLength - array.length)(0.0)).toArray)
    })

    // Convert arrays to vectors, ensuring all are of consistent length
    val dataForLSH = data.withColumn("features", toPaddedVector($"array"))
      .select("id", "pattern", "features")

    dataForLSH
  } 



  
}