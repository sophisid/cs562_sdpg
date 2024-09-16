// Main.scala
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: Main <csvFolderPath> <noisePercentage>")
      sys.exit(1)
    }

    // Dynamically fetch the CSV folder path and noise percentage from command-line arguments
    val csvFolderPath = args(0)
    val noisePercentage = args(1).toDouble

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Schema Discovery with Gaussian Mixture and LSH")
      .master("local[*]")
      .config("spark.neo4j.bolt.url", "bolt://localhost:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "password")
      .getOrCreate()

    // Step 1: Fetch all node labels from Neo4j
    val nodeLabels = DataLoader.getAllNodeLabels(spark)

    // Step 2: Iterate over each node type (label) and process its data
    nodeLabels.foreach { nodeLabel =>
      println(s"Processing data for node label: $nodeLabel")

      // Load data for the current node label from Neo4j
      val neo4jDF = DataLoader.loadFromNeo4j(spark, nodeLabel)

      // Step 3: Introduce noise with the dynamic noise percentage
      val noisyDF = Noise.addNoise(neo4jDF, noisePercentage)

      // Step 4: Apply Gaussian Mixture Model (GMM) for clustering
      val clusteredDF = Clustering.applyGaussianMixture(noisyDF)

      // Step 5: Apply LSH for approximate similarity search
      val lshDF = Clustering.applyLSH(clusteredDF)

      // Display results for the current node label
      println(s"Results for node label: $nodeLabel")
      clusteredDF.show()
      lshDF.show()
    }

    // Stop the Spark session
    spark.stop()
  }
}
