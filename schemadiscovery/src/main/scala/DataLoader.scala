// DataLoader.scala
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

  // Fetch all node labels from Neo4j
  def getAllNodeLabels(spark: SparkSession): Array[String] = {
    val labelsDF = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "password")
      .option("query", "CALL db.labels()")
      .load()

    // Collect the labels into an Array of Strings
    labelsDF.collect().map(row => row.getString(0))
  }

  // Load data for a specific node label from Neo4j
  def loadFromNeo4j(spark: SparkSession, nodeLabel: String): DataFrame = {
    spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "password")
      .option("labels", nodeLabel)
      .load()
  }
}
