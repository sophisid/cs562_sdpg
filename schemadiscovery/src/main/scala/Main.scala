import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._


object Main {

  def createPatternsFromClusters(clusteredDF: DataFrame): List[Pattern] = {
  val patterns = scala.collection.mutable.ListBuffer[Pattern]()
  val clusters = clusteredDF.select("hashes").distinct().collect()
  clusters.foreach { clusterRow =>
    // Extract the hash sequence (as an array) from the first column
    val clusterHash = clusterRow.get(0)

    // Filter the DataFrame based on this hash value
    val clusterData = clusteredDF.filter(col("hashes") === clusterHash)

    val pattern = new Pattern()
    clusterData.collect().foreach { row =>
      val label = row.getAs[String]("label")
      val properties = row.getAs[Map[String, Any]]("properties")
      val node = Node(label, properties)
      pattern.addNode(node)
    }

    patterns += pattern
  }

  patterns.toList
}


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 1) {
      println("Usage: Main <noisePercentage>")
      sys.exit(1)
    }

    val noisePercentage = args(0).toDouble

    val spark = SparkSession.builder()
      .appName("Neo4jSchemaDiscovery")
      .config("spark.master", "local[*]")
      .config("spark.neo4j.bolt.url", "bolt://localhost:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "password")
      .config("spark.rpc.message.maxSize", "512")  // Increase max message size to 512MB
      .config("spark.driver.maxResultSize", "4g")  // Increase max result size
      .config("spark.executor.memory", "8g")       // Increase executor memory
      .config("spark.executor.memoryOverhead", "512") // Increase memory overhead
      .config("spark.task.maxSize", "4m")
      .getOrCreate()

    val nodeLabels = DataLoader.getAllNodeLabels()

    nodeLabels.foreach { nodeLabel =>
      println(s"Processing data for node label: $nodeLabel")

      val nodes = DataLoader.loadFromNeo4j(nodeLabel)
      val noisyNodes = Noise.addNoise(nodes, noisePercentage)
      val nodesDF = createDataFrameFromMaps(spark, noisyNodes)

      val featureDF = Clustering.createFeatureVectors(nodesDF).persist()

      // Repartition to reduce task size
      val partitionCount = Math.max((featureDF.count() / 10000).toInt, 100)  // Dynamically set partition count
      val repartitionedDF = featureDF.repartition(partitionCount)

      val lshDF = Clustering.performLSHClustering(repartitionedDF)

      // Create patterns directly from LSH results
      val patterns = createPatternsFromClusters(lshDF)

      // Display the discovered patterns
      patterns.foreach(pattern => println(pattern.toString))
    }

    spark.stop()
  }

  def createDataFrameFromMaps(spark: SparkSession, data: List[Map[String, Any]]): DataFrame = {
    val keys = data.flatMap(_.keys).distinct
    val schema = StructType(keys.map(StructField(_, StringType, nullable = true)))

    val rows = data.map { map =>
      val values = keys.map(key => map.getOrElse(key, null).asInstanceOf[String])
      Row(values: _*)
    }

    spark.createDataFrame(rows.asJava, schema)
  }
}
