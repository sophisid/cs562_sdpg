import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Neo4jSchemaDiscovery")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Load all nodes with labels
    val nodesDF = DataLoader.loadAllNodes(spark).cache()

    // Matrix of properties
    val binaryMatrixDF = DataProcessor.createBinaryMatrix(nodesDF).cache()

    // LSH clustering
    val lshDF = Clustering.performLSHClustering(binaryMatrixDF).cache()

    // Create patterns from clusters and create mapping nodeId -> clusterLabel
    val (patterns, nodeIdToClusterLabel) = Clustering.createPatternsFromClusters(lshDF)

    // **Edge Discovery Starts Here**

    // Load relationships
    val relationshipsDF = DataLoader.loadAllRelationships(spark)

    // Create edges
    val edges = Clustering.createEdgesFromRelationships(relationshipsDF, nodeIdToClusterLabel)

    // Integrate edges into patterns
    val updatedPatterns = Clustering.integrateEdgesIntoPatterns(edges, patterns)

    // Display the updated patterns
    updatedPatterns.foreach { pattern =>
      println(pattern.toString)
    }

    // **Edge Discovery Ends Here**

    // Prepare Data for Evaluation
    val predictedLabelsDF = nodeIdToClusterLabel.toSeq.toDF("_nodeId", "predictedClusterLabel")

    val nodesWithLabelsDF = nodesDF
      .select($"_nodeId".cast(LongType), $"_labels")

    val evaluationDF = nodesWithLabelsDF.join(predictedLabelsDF, "_nodeId")

    evaluationDF.cache()
    evaluationDF.count() // Trigger caching

    // Compute metrics using the new method
    ClusteringEvaluation.computeMetricsWithoutPairwise(evaluationDF)

    spark.stop()
  }
}
