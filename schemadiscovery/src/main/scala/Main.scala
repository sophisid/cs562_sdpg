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

  val predefinedLabels = List("Person", "Place", "Event", "Organization", "Location", "Product", "Company", "City", "Country")

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
        val noiseLevel = 0.00001 // Minimal noise
        val noisyDataset = Noise.addNoise(dataset, noiseLevel)

        // Detect patterns in the dataset
        val patterns = DataToPattern.extractPatterns(noisyDataset)

        // Add the patterns and rows of the current file to the map of all patterns
        DataToPattern.addPatternsAndRows(allPatterns, patterns)

        // Save the updated patterns and rows to disk
        savePatternsAndRows(allPatterns, patternsFile)
      }

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

      // Group data by hashes and collect lists of ids and patterns
      val groupedData = hashedData.groupBy("hashes")
        .agg(
          collect_list(col("id")).alias("grouped_ids"),
          collect_list(col("pattern")).alias("grouped_patterns")
        )

      val uniquePatterns = groupedData.select("grouped_patterns").distinct()
      val mergedPatterns = uniquePatterns.as[Seq[String]].map(normalizeAndMergePatterns)
        .distinct()
        .toDF("final_patterns")

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

      println("\nGround truth Patterns:")
      groundTruth.foreach(pattern => println(pattern.mkString(", ")))

      println("-------------------------------------------------------------------")

      println("\nDetected Patterns:")
      detectedPatterns.foreach(pattern => println(pattern.mkString(", ")))

      computeAndPrintMetrics(groundTruth, detectedPatterns)

      // Find optional properties and create updated patterns
      val updatedPatterns = findOptionalPropertiesAndCreatePatterns(allPatterns)
      updatedPatterns.foreach { pattern =>
        println(s"Pattern: ${pattern.nodeLabel}")
        println(s"Properties: ${pattern.properties.mkString(", ")}")
        println(s"Edges: ${pattern.edges.mkString(", ")}")
      }

      // Infer types
      files.foreach { file =>
        val dataset = loadAndProcessFile(spark, file)
        val types = inferTypes(spark, dataset)
        println(s"\nInferred Types for $file:")
        types.foreach { case (property, propertyType) =>
          println(s"$property: $propertyType")
        }
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

        val nodeLabel = extractNodeLabelFromPattern(pattern, predefinedLabels)
        val properties = extractPropertiesFromPattern(pattern)
        val edges = extractEdgesFromPattern(pattern)

        println(s"Extracted Node Label: $nodeLabel")
        println(s"Extracted Properties: ${properties.mkString(", ")}")
        println(s"Extracted Edges: ${edges.mkString(", ")}")

        println(s"Node:")
        println(s"  - Label: $nodeLabel")
        if (properties.nonEmpty) {
          println(s"  - Properties: {${properties.map(prop => s"""$prop: "STRING"""").mkString(", ")}}")
        } else {
          println(s"  - Properties: {}")
        }

        if (edges.nonEmpty) {
          println(s"  - Edges: {${edges.map(edge => s"""(${edge._1} -> ${edge._2})""").mkString(", ")}}")
        } else {
          println(s"  - Edges: {}")
        }

        println(" ")
      }
    }
  }

  def extractNodeLabelFromPattern(pattern: String, predefinedLabels: List[String]): String = {
    // Check if the pattern contains any of the predefined labels
    predefinedLabels.find(label => pattern.toLowerCase.contains(label.toLowerCase)) match {
      case Some(matchedLabel) => matchedLabel
      case None => "UnknownNodeLabel"
    }
  }


  def extractPropertiesFromPattern(pattern: String): Seq[String] = {
    val mapStart = pattern.indexOf("HashMap(") match {
      case -1 => pattern.indexOf("Map(")
      case idx => idx
    }
    val mapEnd = pattern.indexOf(')', mapStart)
    if (mapStart > -1 && mapEnd > mapStart) {
      val propertiesString = pattern.substring(mapStart + 7, mapEnd) // +7 to skip "HashMap(" or "Map("
      if (propertiesString.nonEmpty) {
        propertiesString.split(", ").map(_.split("->")(0).trim).toSeq
      } else {
        Seq.empty
      }
    } else {
      Seq.empty
    }
  }

  def extractEdgesFromPattern(pattern: String): Set[(String, String)] = {
    val setStart = pattern.indexOf("Set(")
    val setEnd = pattern.indexOf(')', setStart)
    if (setStart > -1 && setEnd > setStart) {
      val edgesString = pattern.substring(setStart + 4, setEnd) // +4 to skip "Set("
      if (edgesString.nonEmpty) {
        edgesString.split(", ").map { edge =>
          val edgeTuple = edge.stripPrefix("(").stripSuffix(")").split("->").map(_.trim)
          if (edgeTuple.length == 2) (edgeTuple(0), edgeTuple(1)) else ("", "")
        }.filterNot(edge => edge._1.isEmpty && edge._2.isEmpty).toSet
      } else {
        Set.empty
      }
    } else {
      Set.empty
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

def findOptionalPropertiesAndCreatePatterns(allPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]): Seq[Pattern] = {
  val propertyCounts = mutable.Map[String, mutable.Map[String, Int]]()
  val totalCounts = mutable.Map[String, Int]()

  allPatterns.foreach { case (pattern, instances) =>
    val nodeLabel = pattern.nodeLabel
    totalCounts(nodeLabel) = totalCounts.getOrElse(nodeLabel, 0) + instances.size
    instances.foreach { instance =>
      instance.keys.foreach { property =>
        if (!propertyCounts.contains(nodeLabel)) {
          propertyCounts(nodeLabel) = mutable.Map[String, Int]()
        }
        val counts = propertyCounts(nodeLabel)
        counts(property) = counts.getOrElse(property, 0) + 1
      }
    }
  }

  propertyCounts.map { case (nodeLabel, counts) =>
    val totalInstances = totalCounts(nodeLabel)
    val propertiesWithOptionality = counts.map { case (property, count) =>
      property -> (count < totalInstances)
    }.toMap

    Pattern(nodeLabel, propertiesWithOptionality, allPatterns.keys.find(_.nodeLabel == nodeLabel).map(_.edges).getOrElse(Set.empty))
  }.toSeq
}


  def inferTypes(spark: SparkSession, dataset: DataFrame): Map[String, String] = {
    import spark.implicits._

    val inferredSchema = dataset.schema.fields.map { field =>
      val columnName = field.name
      val sampleData = dataset.select($"`$columnName`").take(1000).map(_.get(0))
      val inferredType = sampleData.collect {
        case v if v.isInstanceOf[Int] => "INT"
        case v if v.isInstanceOf[Long] => "LONG"
        case v if v.isInstanceOf[Double] => "DOUBLE"
        case v if v.isInstanceOf[Float] => "FLOAT"
        case v if v.isInstanceOf[Boolean] => "BOOLEAN"
        case _ => "STRING"
      }.groupBy(identity).maxBy(_._2.size)._1 // Most frequent type

      columnName -> inferredType
    }.toMap

    inferredSchema
  }
}
