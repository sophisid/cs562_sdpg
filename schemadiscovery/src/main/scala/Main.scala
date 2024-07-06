import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel, Normalizer}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, DataTypes}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import java.nio.file.{Files, Paths}
import java.io.{ObjectOutputStream, ObjectInputStream, FileInputStream, FileOutputStream}
import java.security.MessageDigest
import com.models.Pattern
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

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
    val startTime = System.nanoTime()

    try {
      // Directory path to process
      val directory = "../datasets/LDBC/ldbc_inputs1/tmp/"

      // File to save and load patterns and rows
      val patternsFile = "patterns_and_rows.ser"
      val mergedPatternsFile = "merged_patterns.parquet"

      // Load existing merged patterns if they exist and print them
      if (Files.exists(Paths.get(mergedPatternsFile))) {
        val existingMergedPatterns = spark.read.parquet(mergedPatternsFile)
        // println("Existing Merged Patterns:")
        // existingMergedPatterns.show(false)
      }

      // Load existing patterns and rows if the file exists
      val allPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]] = if (Files.exists(Paths.get(patternsFile))) {
        convertToLinkedHashMap(loadPatternsAndRows(patternsFile))
      } else {
        mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]()
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

      // DataToPattern.printSortedPatterns(allPatterns)

      val lshStartTime = System.nanoTime()

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
      // mergedPatterns.show(false)

      val lshEndTime = System.nanoTime()
      val lshDuration = (lshEndTime - lshStartTime) / 1e9d

      // Load existing merged patterns if they exist
      val existingMergedPatterns = if (Files.exists(Paths.get(mergedPatternsFile))) {
        spark.read.parquet(mergedPatternsFile).as[String].collect().toSet
      } else {
        Set[String]()
      }

      // Combine existing merged patterns with new ones, ensuring no duplicates
      val updatedMergedPatterns = (existingMergedPatterns ++ mergedPatterns.as[String].collect().toSet).toSeq.toDF("final_patterns")

      // Save the updated merged patterns to disk
      updatedMergedPatterns.write.mode("overwrite").parquet(mergedPatternsFile)

      println(s"LSH running time: $lshDuration seconds")

      val endTime = System.nanoTime()
      val totalDuration = (endTime - startTime) / 1e9d
      println(s"Total running time: $totalDuration seconds")

      // Print final patterns in the desired format
      printFinalPatterns(updatedMergedPatterns)
      
      // Compute and print metrics
      val groundTruth = extractPatternsFromAllPatterns(allPatterns)
      val detectedPatterns = extractPatternsFromMergedPatterns(updatedMergedPatterns)
      // val groundTruth = Set(
      //   Set("birthday", "locationIP", "lastName", "firstName", "creationDate", "gender"),
      //   Set("birthday", "locationIP", "firstName", "browserUsed", "creationDate", "gender"),
      //   Set("birthday", "locationIP", "lastName", "firstName", "browserUsed", "creationDate"),
      //   Set("birthday", "locationIP", "lastName", "firstName")
      //   // Add all other ground truth patterns here
      // )

      // val detectedPatterns = Set(
      //   Set("birthday", "lastName", "firstName", "creationDate", "gender"),
      //   Set("birthday", "locationIP", "firstName", "browserUsed", "creationDate", "gender"),
      //   Set("birthday", "locationIP", "lastName", "firstName", "browserUsed", "creationDate"),
      //   Set("birthday", "locationIP", "lastName", "firstName", "gender"),
      //    Set("birthday", "locationIP", "lastName", "firstName", "tmp")
      // )

      println("\nGround truth Patterns:")
      groundTruth.foreach(pattern => println(pattern.mkString(", ")))

      println("-------------------------------------------------------------------")

      println("\nDetected Patterns:")
      detectedPatterns.foreach(pattern => println(pattern.mkString(", ")))

      // computeAndPrintMetrics(groundTruth.toList, detectedPatterns.toList)
      computeAndPrintMetrics(groundTruth, detectedPatterns)

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

  def savePatternsAndRows(patterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]], filePath: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(filePath))
    oos.writeObject(patterns)
    oos.close()
  }

def loadPatternsAndRows(filePath: String): mutable.LinkedHashMap[Pattern, List[Map[String, Any]]] = {
  val ois = new ObjectInputStream(new FileInputStream(filePath))
  val loadedObject = ois.readObject()
  ois.close()

  loadedObject match {
    case map: mutable.Map[_, _] =>
      val hashMap = map.asInstanceOf[mutable.Map[Pattern, List[Map[String, Any]]]]
      convertToLinkedHashMap(hashMap)
    case _ =>
      throw new ClassCastException("Loaded object is not a Map")
  }
}

def convertToLinkedHashMap(hashMap: mutable.Map[Pattern, List[Map[String, Any]]]): mutable.LinkedHashMap[Pattern, List[Map[String, Any]]] = {
  val linkedHashMap = new mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]()
  hashMap.foreach { case (k, v) => linkedHashMap.put(k, v) }
  linkedHashMap
  }

def printFinalPatterns(mergedPatterns: DataFrame): Unit = {
  mergedPatterns.collect().foreach { row =>
    val patterns = row.getString(0).split('|')
    patterns.foreach { pattern =>
      println(s"Raw pattern: $pattern") // Print the raw pattern for debugging

      val nodeLabel = extractNodeLabel(pattern)
      val properties = extractProperties(pattern)

      // println(s"Extracted Node Label: $nodeLabel")
      // println(s"Extracted Properties: ${properties.mkString(", ")}")

      println(s"Node:")
      println(s"  - Label: $nodeLabel")
      if (properties.nonEmpty) {
        println(s"  - Properties: {${properties.map(prop => s"""$prop: "STRING"""").mkString(", ")}}")
      } else {
        println(s"  - Properties: {}")
      }
      println(" ")
    }
  }
}

def extractNodeLabel(pattern: String): String = {
  // Assuming the pattern starts with "Pattern(" and ends before the first comma
  val start = pattern.indexOf('(') + 1
  val end = pattern.indexOf(',')
  if (start >= 0 && end > start) {
    pattern.substring(start, end).trim
  } else {
    "Unknown"
  }
}

def extractProperties(pattern: String): Seq[String] = {
  // Assuming properties are between "Set(" and the first closing parenthesis after that
  val setStart = pattern.indexOf("Set(")
  val setEnd = pattern.indexOf(')', setStart)
  if (setStart >= 0 && setEnd > setStart) {
    val propertiesString = pattern.substring(setStart + 4, setEnd)
    if (propertiesString.nonEmpty) {
      propertiesString.split(", ").map(_.trim).toSeq
    } else {
      Seq.empty
    }
  } else {
    Seq.empty
  }
}

  def extractPatternsFromAllPatterns(allPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]): List[Set[String]] = {
    allPatterns.keys.map(pattern => pattern.toString.split('|').map(_.trim).toSet).toList
  }


  def extractPatternsFromMergedPatterns(mergedPatterns: DataFrame): List[Set[String]] = {
    val uniquePatterns = mergedPatterns.collect().flatMap { row =>
      row.getString(0).split('|').map(_.trim).toSet
    }.toSet

    uniquePatterns.map(Set(_)).toList
  }



  def computeAndPrintMetrics(groundTruth: List[Set[String]], detectedPatterns: List[Set[String]]): Unit = {
    val precision = Metrics.overallPrecision(groundTruth, detectedPatterns)
    val recall = Metrics.overallRecall(groundTruth, detectedPatterns)
    val f1 = Metrics.f1Score(precision, recall)

    println(" ")
    println("Metrics:")
    println(s"Precision: $precision")
    println(s"Recall: $recall")
    println(s"F1 Score: $f1")
  }


}
