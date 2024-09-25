import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, DenseVector}
import org.apache.spark.sql.expressions.UserDefinedFunction


object Clustering {

  // Function to perform LSH clustering
  def performLSHClustering(df: DataFrame): DataFrame = {
    // Assemble the binary features into a vector
    val assembler = new VectorAssembler()
      .setInputCols(df.columns.filterNot(_ == "_nodeId"))
      .setOutputCol("features")

    val featureDF = assembler.transform(df)

    // Apply MinHash LSH
    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(featureDF)
    val lshDF = model.transform(featureDF)
    // Μετά το lshDF
    println(s"LSH clustering completed. DataFrame has ${lshDF.count()} rows.")
    lshDF.select("hashes").show(5, truncate = false)

    lshDF
  }

  // Function to create patterns from clusters
  def createPatternsFromClusters(df: DataFrame): Array[Pattern] = {
    // Exclude certain columns
    val excludeCols = Set("_nodeId", "features", "hashes", "hashKey")

    // Create a hashKey column by converting the hashes to a string
    val dfWithHashKey = df.withColumn("hashKey", udf((hashes: Seq[Vector]) => {
      hashes.map(_.toArray.mkString("_")).mkString("_")
    }).apply(col("hashes")))

    // Group by the hashKey to get clusters
    val clustersDF = dfWithHashKey.groupBy("hashKey")
      .agg(collect_list(col("_nodeId")).as("nodeIds"))

    // For each cluster, find common properties
    val patterns = clustersDF.collect().map { row =>
      val nodeIds = row.getAs[Seq[String]]("nodeIds")
      val hashKey = row.getAs[String]("hashKey")

      // Filter the original DataFrame to get nodes in the cluster
      val clusterNodesDF = dfWithHashKey.filter(col("hashKey") === hashKey)

      // Find common properties (columns where all values are 1)
      val propertyCols = df.columns.filterNot(colName => excludeCols.contains(colName))

      val commonProperties = propertyCols.filter { colName =>
        val colSum = clusterNodesDF.agg(sum(col(colName))).first().getLong(0)
        colSum == clusterNodesDF.count()
      }

      // Create a Node with the common properties
      val node = Node(
        label = s"Cluster_$hashKey",
        properties = commonProperties.map(prop => prop -> 1).toMap
      )

      val pattern = new Pattern(nodes = List(node))
      pattern
    }
    println(s"Total patterns created: ${patterns.length}")
    println("Sample patterns:")
    patterns.take(5).foreach(pattern => println(pattern.toString))
    patterns
  }
}
