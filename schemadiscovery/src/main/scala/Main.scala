import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import org.apache.log4j.{Level, Logger}
import scala.collection.View
import scala.collection.MapView

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
      val files = listFiles(directory, ".csv")

      // HashMap to accumulate all patterns
      val allPatterns = mutable.HashMap[String, Set[String]]()
      val allEdges = mutable.HashMap[String, mutable.Set[(String, String)]]()
      val connectedNodeTypes = mutable.HashMap[String, Set[String]]()
      val instanceVectors = mutable.HashMap[String, Set[String]]()

      // Process each file
      files.foreach { file =>
        println(s"Processing file: $file")
        processFile(spark, file)


      }


    } finally {
      spark.stop()
    }
  }

  def listFiles(directory: String, extension: String): List[String] = {
    import scala.jdk.CollectionConverters._
    val path = Paths.get(directory)
    val files = Files.list(path).iterator().asScala
      .filter(_.toString.endsWith(extension))
      .map(_.toString)
      .toList
    files
  }

  def processFile(spark: SparkSession, filePath: String): Unit = {
    // Load the dataset from the provided CSV file
    val dataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .csv(filePath)
    
    // Add noise to the dataset if needed
    val noiseLevel = 0.1 // 10% noise
    val noisyDataset = Noise.addNoise(dataset, noiseLevel) // Assume Noise.scala is implemented
    noisyDataset.show(5) // Shows 5 rows of noisy data

    // val instanceVectors = findInstanceVectors(e_i)
    
    // findInstanceVectors(e_i)

    // Processing the DataFrame to get the desired map
    val patternOfFile = createTypeToValuesMap(noisyDataset)

    // Outputting the results for demonstration purposes
    patternOfFile.foreach { case (key, values) =>
      println(s"Key: ${key.mkString(", ")} -> Values: ${values.map(_.mkString(", ")).mkString("[", "; ", "]")}")
    }



  }


  // def findInstanceVectors(dataset: DataFrame): Map[Vector[String], Vector[String]] = {
  // def findInstanceVectors(df: DataFrame): Unit = {
  //   val generalPropertiesOfFile = df.columns
  //   val nodeIDType = generalPropertiesOfFile.head
  //   val generalEdgesTypes = generalPropertiesOfFile.tail

  //   val edgeCols = generalEdgesTypes.filter(_.endsWith("_id"))

  //   val nodesWithPropertiesAndEdges = df.collect().map { row =>
  //     val nodeId = row.getString(row.fieldIndex(nodeIDType))

  //     val properties = generalEdgesTypes.filterNot(edgeCols.contains).map { col =>
  //       col -> row.getString(row.fieldIndex(col))
  //     }.toMap

  //     val edges = edgeCols.flatMap { col =>
  //       Option(row.getString(row.fieldIndex(col))).map(targetNode => col -> targetNode)
  //     }.to(mutable.Set)
  //     nodeId -> (properties, edges)
  //   }.groupBy(_._1).mapValues(_.map(_._2).reduce((t1, t2) => {
  //     val mergedProperties = t1._1 ++ t2._1
  //     val mergedEdges = t1._2 ++ t2._2
  //     (mergedProperties, mergedEdges)
  //   }))


  // }

 def createTypeToValuesMap(df: DataFrame): Map[Seq[String], Seq[Seq[Any]]] = {
    // Get column names
    val columnNames = df.columns

    // Collect the rows and transform them into the desired map
    val rows = df.collect().map(row => 
      columnNames.map(col => row.getAs[Any](col)).toSeq
    ).toSeq

    // The map has only one key, which is a tuple of all column names, and the values are the rows transformed
    Map(columnNames.toSeq -> rows)
  }

}
