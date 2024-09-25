import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Neo4jSchemaDiscovery")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val startTime = System.nanoTime()
    // Get the noise percentage from arguments or default to 0.0
    val noisePercentage = if (args.length > 0) args(0).toDouble else 0.0

    // Load all nodes without labels
    // val nodesDF = DataLoader.loadAllNodes(spark).cache()
    val nodesDF = DataLoader.loadAllNodes(spark).repartition(10).cache()


    // Create binary matrix of properties
    val binaryMatrixDF = DataProcessor.createBinaryMatrix(nodesDF).cache()

    // Perform LSH clustering
    val lshDF = Clustering.performLSHClustering(binaryMatrixDF)

    // Create patterns from clusters
    val patterns = Clustering.createPatternsFromClusters(lshDF)

    // Print patterns
    patterns.foreach(pattern => println(pattern.toString))
    val endTime = System.nanoTime()
    println(s"Execution time: ${(endTime - startTime) / 1e9} seconds")
    spark.stop()
  }
}
