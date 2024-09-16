import org.neo4j.driver.{AuthTokens, GraphDatabase, Session}
import scala.jdk.CollectionConverters._  // Import JavaConverters to convert java.util.List to Scala collections

object DataLoader {

  // Function to fetch all node labels from Neo4j using the Java Driver
  def getAllNodeLabels(): List[String] = {
    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"))
    val session: Session = driver.session()

    val result = session.run("CALL db.labels()")

    // Convert Java List to Scala List using asScala
    val labels = result.list().asScala.map(_.get(0).asString).toList

    session.close()
    driver.close()

    labels
  }

  // Load data for a specific node label using the Java Driver
  def loadFromNeo4j(label: String): List[Map[String, Any]] = {
    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"))
    val session: Session = driver.session()

    val result = session.run(s"MATCH (n:$label) RETURN n")

    // Convert Java List to Scala List and process the records
    val nodes = result.list().asScala.map(record => {
      val node = record.get("n").asNode()
      node.keys().asScala.map(key => key -> node.get(key).asObject()).toMap
    }).toList

    session.close()
    driver.close()

    nodes
  }
}
