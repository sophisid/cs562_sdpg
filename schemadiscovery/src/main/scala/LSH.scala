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
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")

    brp.fit(data)
  }

  def prepareDataForLSH(allPatterns: mutable.Map[Seq[String], mutable.ListBuffer[Seq[Any]]], spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Convert the map to a DataFrame with an array of Doubles
    val data = allPatterns.flatMap { case (_, listBuffer) =>
      listBuffer.map { row =>
        row.map {
          case num: Number => num.doubleValue()
          case other => other.hashCode.toDouble
        }.toArray
      }
    }.toSeq.toDF("array")

    // Find the maximum length of any vector
    val maxLength = data.select(max(size($"array"))).as[Int].collect()(0)

    // UDF to pad vectors to the maximum length
    val toPaddedVector = udf((array: Seq[Double]) => {
      Vectors.dense((array ++ Array.fill(maxLength - array.length)(0.0)).toArray)
    })

    // Convert arrays to vectors and pad them to ensure consistent length
    val dataForLSH = data.withColumn("features", toPaddedVector($"array")).drop("array")

    dataForLSH
  }


  
}