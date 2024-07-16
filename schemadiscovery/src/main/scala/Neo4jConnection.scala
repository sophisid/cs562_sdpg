import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}

object Neo4jConnection {
  val uri = "neo4j://localhost:7687"
  val user = "neo4j"
  val password = "password"

  val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))

  def close(): Unit = {
    driver.close()
  }

  def executeQuery(query: String): Unit = {
    val session: Session = driver.session()
    try {
      val result = session.run(query)
      while (result.hasNext) {
        val record = result.next()
        println(record)
      }
    } finally {
      session.close()
    }
  }
}
