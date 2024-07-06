
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel, Normalizer}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, DataTypes}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import java.nio.file.{Files, Paths}
import java.io.{ObjectOutputStream, ObjectInputStream, FileInputStream, FileOutputStream}
import com.models.Pattern

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

      // File to save and load patterns and rows
      val patternsFile = "patterns_and_rows.ser"
      val mergedPatternsFile = "merged_patterns.parquet"

      // Load existing merged patterns if they exist and print them
      if (Files.exists(Paths.get(mergedPatternsFile))) {
        val existingMergedPatterns = spark.read.parquet(mergedPatternsFile)
        println("Existing Merged Patterns:")
        existingMergedPatterns.show(false)
      }

      // Load existing patterns and rows if the file exists
      val allPatterns: mutable.HashMap[Pattern, List[Map[String, Any]]] = if (Files.exists(Paths.get(patternsFile))) {
        loadPatternsAndRows(patternsFile)
      } else {
        mutable.HashMap[Pattern, List[Map[String, Any]]]()
      }

      // Get list of CSV files in the directory
      val files = listFiles(directory)

      // Process each file
      files.foreach { file =>
        println(s"Processing file: $file")
        val dataset = loadAndProcessFile(spark, file)
        val noiseLevel = 0.1 // 10% noise
        val noisyDataset = Noise.addNoise(dataset, noiseLevel)

        // Detect patterns in the dataset
        val patterns = DataToPattern.extractPatterns(noisyDataset)

        // Add the patterns and rows of the current file to the map of all patterns
        DataToPattern.addPatternsAndRows(allPatterns, patterns)

        // Save the updated patterns and rows to disk
        savePatternsAndRows(allPatterns, patternsFile)
      }

      DataToPattern.printSortedPatterns(allPatterns)

      // Convert the map to a DataFrame and then to a dataset for LSH
      val dataForLSH = LSH.prepareDataForLSH(allPatterns, spark)

      // // Normalize feature vectors
      // val normalizer = new Normalizer()
      //   .setInputCol("features")
      //   .setOutputCol("normFeatures")
      //   .setP(2.0) // L2 norm

      // val normalizedData = normalizer.transform(dataForLSH)
      // val model = LSH.setupLSH(normalizedData)
      // val hashedData = model.transform(normalizedData)
      // // hashedData.show(false)

      // // Group data by hashes and collect lists of ids and patterns
      // val groupedData = hashedData.groupBy("hashes")
      //   .agg(
      //     collect_list(col("id")).alias("grouped_ids"),
      //     collect_list(col("pattern")).alias("grouped_patterns")
      //   )

      // val uniquePatterns = groupedData.select("grouped_patterns").distinct()
      // uniquePatterns.show(false)
      // val mergedPatterns = uniquePatterns.as[Seq[String]].map(normalizeAndMergePatterns)
      //   .distinct()
      //   .toDF("final_patterns")
      // mergedPatterns.show(false)

      // // Load existing merged patterns if they exist
      // val existingMergedPatterns = if (Files.exists(Paths.get(mergedPatternsFile))) {
      //   spark.read.parquet(mergedPatternsFile).as[String].collect().toSet
      // } else {
      //   Set[String]()
      // }

      // // Combine existing merged patterns with new ones, ensuring no duplicates
      // val updatedMergedPatterns = (existingMergedPatterns ++ mergedPatterns.as[String].collect().toSet).toSeq.toDF("final_patterns")

      // // Save the updated merged patterns to disk
      // updatedMergedPatterns.write.mode("overwrite").parquet(mergedPatternsFile)

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
    patterns.flatMap(_.split("\\|")).map(_.trim).toSet.toSeq.sorted.mkString("|")
  }

  def savePatternsAndRows(patterns: mutable.HashMap[Pattern, List[Map[String, Any]]], filePath: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(filePath))
    oos.writeObject(patterns)
    oos.close()
  }

  def loadPatternsAndRows(filePath: String): mutable.HashMap[Pattern, List[Map[String, Any]]] = {
    val ois = new ObjectInputStream(new FileInputStream(filePath))
    val patterns = ois.readObject().asInstanceOf[mutable.HashMap[Pattern, List[Map[String, Any]]]]
    ois.close()
    patterns
  }
}
