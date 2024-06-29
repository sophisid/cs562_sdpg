import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, DataTypes}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Schema Discovery for Property Graphs")
      .config("spark.master", "local")
      .getOrCreate()

    // Set the log level to WARN to see fewer details
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    

    try {
      // Directory path to process
      val directory = "../datasets/LDBC/ldbc_inputs1/tmp"

      // Get list of CSV files in the directory
      val files = listFiles(directory)

      // Initialize a map to accumulate all unique patterns and their corresponding rows
      val allPatterns = mutable.Map[Seq[String], mutable.ListBuffer[Seq[Any]]]()

      // Process each file
      files.foreach { file =>
        println(s"Processing file: $file")
        val dataset = loadAndProcessFile(spark, file)
        val noiseLevel = 0.1 // 10% noise
        val noisyDataset = Noise.addNoise(dataset, noiseLevel)

        // Detect patterns in the dataset
        val patterns = DataToPattern.detectPatterns(noisyDataset)

        // Add the patterns and rows of the current file to the map of all patterns
        DataToPattern.addPatternsAndRows(allPatterns, patterns)

      }

      // Sort and print all distinct patterns found
      DataToPattern.printSortedPatterns(allPatterns)

      val dataForLSH = prepareDataForLSH(allPatterns, spark)
      val model = setupLSH(dataForLSH)
      val hashedData = model.transform(dataForLSH)
      hashedData.show(false)


    } finally {
      spark.stop()
    }
  }

  def listFiles(directory: String): List[String] = {
    val dir = new java.io.File(directory)
    dir.listFiles.filter(_.isFile).map(_.getAbsolutePath).toList
  }

  def loadAndProcessFile(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .csv(filePath)
  }

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
