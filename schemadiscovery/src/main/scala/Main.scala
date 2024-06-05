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
      val directory = "../../../../datasets/"

      // Get list of CSV files in the directory
      val files = listFiles(directory, ".csv")

      // Initialize a map to accumulate all patterns
      var allPatterns = Map[Seq[String], Seq[Seq[Any]]]()
      allPatterns.foreach { case (key, values) =>
        println(s"Key: ${key.mkString(", ")}") // -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
      }

      // Process each file
      files.foreach { file =>
        println(s"\nProcessing file: $file")
        val dataset = loadAndProcessFile(spark, file)

        // val noiseLevel = 0.95 // 10% noise
        // val noisyDataset = Noise.addNoise(dataset, noiseLevel)
        // val patternOfFile = createTypeToValuesMap(noisyDataset)

        // val patternOfFile = createTypeToValuesMap(dataset)
        // println(s"petternOfFile for the $file :")
        // patternOfFile.foreach { case (key, values) =>
        //   println(s"Key: ${key.mkString(", ")}") // -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
        // }

        val (completeMap, incompleteMaps) = createTypeToValuesMap(dataset)
        completeMap.foreach { case (key, values) =>
          println(s"Key: ${key.mkString(", ")}") // -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
        }
        incompleteMaps.foreach { case (key, values) =>
          println(s"Key: ${key.mkString(", ")}") // -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
        }
        // println("Complete Map: " + completeMap)
        // println("Incomplete Maps: " + incompleteMaps)

        allPatterns = mergePatterns(allPatterns, completeMap)
        allPatterns = mergePatterns(allPatterns, incompleteMaps)

      }

      // Print final patterns
      allPatterns.foreach { case (key, values) =>
        println(s"Key: ${key.mkString(", ")}") // -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
      }
      
    } finally {
      spark.stop()
    }
  }

  def listFiles(directory: String, extension: String): List[String] = {
    val path = Paths.get(directory)
    Files.list(path).iterator().asScala
      .filter(_.toString.endsWith(extension))
      .map(_.toString)
      .toList
  }

  def loadAndProcessFile(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .csv(filePath)
  }

  // def createTypeToValuesMap(df: DataFrame): Map[Seq[String], Seq[Seq[Any]]] = {
  //   val columnNames = df.columns
  //   val rows = df.collect().map(row => 
  //     columnNames.map(col => row.getAs[Any](col)).toSeq
  //   ).toSeq
  //   Map(columnNames.toSeq -> rows)
  // }

  def createTypeToValuesMap(df: DataFrame): (Map[Seq[String], Seq[Seq[Any]]], Map[Seq[String], Seq[Seq[Any]]]) = {
    val columnNames: Seq[String] = df.columns.toSeq
    val rows: Seq[Seq[Any]] = df.collect().map(row => 
      columnNames.map(col => row.getAs[Any](col))
    ).toSeq

    val completeMap: Map[Seq[String], Seq[Seq[Any]]] = Map(columnNames -> rows)

    val incompleteRows = rows.flatMap { row =>
      val nonEmptyColumns = columnNames.zip(row).collect {
        case (colName, value) if value != null && value.toString.nonEmpty => colName
      }
      if (nonEmptyColumns.length < columnNames.length) {
        val filteredRow = row.zip(columnNames).collect {
          case (value, colName) if nonEmptyColumns.contains(colName) => value
        }
        Some(nonEmptyColumns -> filteredRow)
      } else {
        None
      }
    }.groupBy(_._1).mapValues(_.map(_._2).toSeq).toMap

    (completeMap, incompleteRows)
  }


  def mergePatterns(map1: Map[Seq[String], Seq[Seq[Any]]], map2: Map[Seq[String], Seq[Seq[Any]]]): Map[Seq[String], Seq[Seq[Any]]] = {
    (map1.keySet ++ map2.keySet).map { key =>
      key -> (map1.getOrElse(key, Seq.empty) ++ map2.getOrElse(key, Seq.empty))
    }.toMap
  }
}
