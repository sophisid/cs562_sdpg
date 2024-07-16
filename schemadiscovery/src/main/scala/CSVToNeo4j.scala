import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object CSVToNeo4j {
  def insertData(data: DataFrame, label: String): Unit = {
    val session = Neo4jConnection.driver.session()
    try {
      data.collect().foreach { row =>
        val properties = row.schema.fieldNames.map { field =>
          val value = Option(row.getAs[Any](field)) match {
            case Some(s: String) => s"'${s.replace("'", "\\'")}'"
            case Some(other) => other.toString
            case None => "null"
          }
          s"$field: $value"
        }.mkString(", ")
        val query =
          s"""
             |CREATE (n:$label { $properties })
           """.stripMargin
        session.run(query)
      }
    } finally {
      session.close()
    }
  }
}
