import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.immutable.HashMap
import org.apache.log4j.{Level, Logger}
import scala.jdk.CollectionConverters._

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

      // Initialize a map to accumulate all patterns
      val allPatterns = mutable.Set[Seq[String]]()

      // Process each file
      files.foreach { file =>
        println(s"Processing file: $file")
        val dataset = loadAndProcessFile(spark, file)
        val noiseLevel = 0.5 // 10% noise
        val noisyDataset = Noise.addNoise(dataset, noiseLevel)

        // val patterns = DataToPattern.extractPatterns(noisyDataset)

        val patterns = detectPatterns(noisyDataset)
        patterns.foreach { case (pattern, count) =>
          println(s"Pattern found $count times: ${pattern.mkString(", ")}")
        }
        // val pattern = extractColumnPattern(noisyDataset)
        // allPatterns += pattern

        

        // val patternOfFile = createTypeToValuesMap(noisyDataset)

        // // Merge current file patterns with the accumulated patterns
        // allPatterns = mergePatterns(allPatterns, patternOfFile)
      }

      // // Print final patterns
      // allPatterns.foreach { case (key, values) =>
      //   println(s"Key: ${key.mkString(", ")} -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
      // }

      allPatterns.zipWithIndex.foreach { case (pattern, index) =>
        println(s"Pattern ${index + 1}: ${pattern.mkString(", ")}")
      }
      
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

  def createTypeToValuesMap(df: DataFrame): Map[Seq[String], Seq[Seq[Any]]] = {
    val columnNames = df.columns
    val rows = df.collect().map(row => 
      columnNames.map(col => row.getAs[Any](col)).toSeq
    ).toSeq
    Map(columnNames.toSeq -> rows)
  }

  def mergePatterns(map1: Map[Seq[String], Seq[Seq[Any]]], map2: Map[Seq[String], Seq[Seq[Any]]]): Map[Seq[String], Seq[Seq[Any]]] = {
    (map1.keySet ++ map2.keySet).map { key =>
      key -> (map1.getOrElse(key, Seq.empty) ++ map2.getOrElse(key, Seq.empty))
    }.toMap
  }

  def extractColumnPattern(df: DataFrame): Seq[String] = {
    df.columns.sorted
  }

  def detectPatterns(df: DataFrame): Map[Seq[String], Int] = {
    val patternCounts = mutable.Map[Seq[String], Int]()

    df.collect().foreach { row =>
      val pattern = row.schema.fields.zipWithIndex.collect {
        case (field, index) if !row.isNullAt(index) => field.name
      }.toSeq

      patternCounts(pattern) = patternCounts.getOrElse(pattern, 0) + 1
    }

    patternCounts.toMap
  }
}
