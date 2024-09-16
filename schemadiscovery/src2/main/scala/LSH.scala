import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable
import com.models.Pattern

object LSH {

  def setupLSH(data: DataFrame): BucketedRandomProjectionLSHModel = {
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(10)
      .setInputCol("features")
      .setOutputCol("hashes")

    brp.fit(data)
  }

  def prepareDataForLSH(allPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]], spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Flatten the map into a sequence and convert to DataFrame
    val data = allPatterns.flatMap { case (pattern, list) =>
      list.zipWithIndex.map { case (row, index) =>
        val featureArray = pattern.properties.keys.map { property =>
          row.get(property) match {
            case Some(num: Number) => num.doubleValue()
            case Some(other) => other.hashCode.toDouble
            case None => 0.0
          }
        }.toArray
        (pattern.toString, index, featureArray)
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
