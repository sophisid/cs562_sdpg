import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Schema Discovery for Property Graphs")
      .config("spark.master", "local")
      .getOrCreate()

    try { 
      //todo take the csv as a parameter
      val dataset = spark.read.option("header", "true").csv("../datasets/LDBC/ldbc_inputs1/place_0_0.csv")
      val noiseLevel = 0.1 // 10% noise
      val noisyDataset = Noise.addNoise(dataset, noiseLevel) // Noise.scala

      noisyDataset.show() // Shows 20 rows of noisy data
    } finally {
      spark.stop()
    }
  }
}
