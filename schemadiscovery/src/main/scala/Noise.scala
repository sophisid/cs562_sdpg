import org.apache.spark.sql.{DataFrame, SparkSession, Encoders}
import scala.util.Random
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object Noise {
  def addNoise(df: DataFrame, noiseLevel: Double): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val schema = df.schema // Get the schema of the DataFrame
    val encoder = RowEncoder(schema) // Create an encoder for Row based on the schema

    val noisyData = df.sample(withReplacement = false, noiseLevel)
      .map(row => {
        org.apache.spark.sql.Row.fromSeq(row.toSeq.map {
          case i: Int => i + Random.nextInt(10) - 5 // Slight random adjustment to integers
          case d: Double => d * (1 + (Random.nextDouble() - 0.5) / 10) // 5% variance for doubles
          case s: String => if (Random.nextBoolean()) s.reverse else s // Reverse strings randomly
          case other => other // Leave other types unchanged
        })
      })(encoder) // Use the created encoder

    // Union the original data with the noisy data
    val noiseFreeCount = df.count() - noisyData.count()
    df.limit(noiseFreeCount.toInt).union(noisyData)
  }
}
