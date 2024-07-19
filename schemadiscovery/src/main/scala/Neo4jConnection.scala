import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Record, Session}
import scala.collection.JavaConverters._

object Neo4jConnection {
  val uri = "bolt://localhost:7687"
  val user = "neo4j"
  val password = "password"

  val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))

  def close(): Unit = {
    driver.close()
  }

  def executeQuery(query: String): Seq[Record] = {
    val session: Session = driver.session()
    try {
      val result = session.run(query)
      result.list().asScala.toSeq // Convert Buffer to Seq
    } finally {
      session.close()
    }
  }
}
