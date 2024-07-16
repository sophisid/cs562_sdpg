import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel, Normalizer}
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

    import spark.implicits._

    try {
      // Directory path to process
      val directory = "../datasets/LDBC/ldbc_inputs1/tmp/"

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

        // Insert the noisy dataset into Neo4j
        CSVToNeo4j.insertData(noisyDataset, "Person")

        // Detect patterns in the dataset
        val patterns = DataToPattern.detectPatterns(noisyDataset)

        // Add the patterns and rows of the current file to the map of all patterns
        DataToPattern.addPatternsAndRows(allPatterns, patterns)
      }

      // Convert the map to a DataFrame and then to a dataset for LSH
      val dataForLSH = LSH.prepareDataForLSH(allPatterns, spark)

      // Normalize feature vectors
      val normalizer = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(2.0) // L2 norm

      val normalizedData = normalizer.transform(dataForLSH)
      val model = LSH.setupLSH(normalizedData)
      val hashedData = model.transform(normalizedData)
      // hashedData.show(false)

      // Group data by hashes and collect lists of ids and patterns
      val groupedData = hashedData.groupBy("hashes")
        .agg(
          collect_list(col("id")).alias("grouped_ids"),
          collect_list(col("pattern")).alias("grouped_patterns")
        )

      val uniquePatterns = groupedData.select("grouped_patterns").distinct()
      // uniquePatterns.show(false)
      val mergedPatterns = uniquePatterns.as[Seq[String]].map(normalizeAndMergePatterns)
      .distinct()
      .toDF("final_patterns")
      mergedPatterns.show(false)

      // Extract ground truth patterns from allPatterns
      val groundTruthPatterns = allPatterns.keys.map(_.toSet).toList

      // Extract detected patterns from mergedPatterns
      val detectedPatterns = mergedPatterns.collect().map(row => row.getAs[String]("final_patterns").split("\\|").toSet).toList

      // Print ground truth and detected patterns
      println("Ground Truth Patterns:")
      groundTruthPatterns.foreach(println)
      println("-----------------------")
      println("Detected Patterns:")
      detectedPatterns.foreach(println)
      println("-----------------------")

      // Calculate precision, recall, and F1 score
      val overallPrecisionValue = Metrics.overallPrecision(groundTruthPatterns, detectedPatterns)
      val overallRecallValue = Metrics.overallRecall(groundTruthPatterns, detectedPatterns)
      val f1ScoreValue = Metrics.f1Score(overallPrecisionValue, overallRecallValue)

      println(s"Overall Precision: $overallPrecisionValue")
      println(s"Overall Recall: $overallRecallValue")
      println(s"F1 Score: $f1ScoreValue")

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
  
  def normalizeAndMergePatterns(patterns: Seq[String]): String = {
    patterns.flatMap(_.split("\\|")).toSet.toSeq.mkString("|")
  }
  
}
