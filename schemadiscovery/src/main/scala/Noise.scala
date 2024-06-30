import org.apache.spark.sql.{DataFrame, SparkSession, Encoders}
import scala.util.Random
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object Noise {
  def addNoise(df: DataFrame, noisePercentage: Double): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val schema = df.schema
    val encoder = RowEncoder(schema)

    val noisyData = df.map(row => {
      org.apache.spark.sql.Row.fromSeq(row.toSeq.zipWithIndex.map {
        case (value, index) if schema(index).name.toLowerCase.contains("id") => value // Do not alter columns containing "id" or "ID"
        case (value, _) if Random.nextDouble() < noisePercentage => null
        case (value, _) => value
      })
    })(encoder)

    noisyData
  }
}