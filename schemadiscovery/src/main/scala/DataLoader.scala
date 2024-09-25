import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import java.util.HashMap
import scala.collection.JavaConverters._

object DataLoader {

  // Function to load all nodes without labels
  def loadAllNodes(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "password"

    val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session = driver.session()

    println("Loading all nodes from Neo4j")

    // val result = session.run("MATCH (n) RETURN n")
    val result = session.run("MATCH (n) RETURN n LIMIT 1000")
    val nodes = result.list().asScala.map { record =>
      val node = record.get("n").asNode()
      val props = node.asMap().asScala.toMap
      // Include node ID if needed
      props + ("_nodeId" -> node.id().toString)
    }

    session.close()
    driver.close()

    // Collect all unique keys to define the schema
    val allKeys = nodes.flatMap(_.keys).toSet

    // Define the schema based on the keys
    val fields = allKeys.map { key =>
      StructField(key, StringType, nullable = true)
    }.toArray
    val schema = StructType(fields)

    // Convert list of Maps to DataFrame
    val rows = nodes.map { nodeMap =>
      val values = schema.fields.map { field =>
        Option(nodeMap.getOrElse(field.name, null)).map(_.toString).orNull
      }
      Row(values: _*)
    }

    val nodesDF = spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), schema)
    println(s"Total nodes loaded: ${nodesDF.count()}")
    println("Schema of nodesDF:")
    nodesDF.printSchema()
    nodesDF
  }
}
