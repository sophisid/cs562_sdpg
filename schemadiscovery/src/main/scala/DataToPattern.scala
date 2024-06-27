import org.apache.spark.sql.{DataFrame, SparkSession, Encoder, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap

case class Node(id: Long, properties: Map[String, String])
case class Edge(srcId: Long, dstId: Long, properties: Map[String, String])

object DataToPattern {
  import org.apache.spark.sql.Encoders
  implicit val nodeEncoder: Encoder[Node] = Encoders.product[Node]
  implicit val edgeEncoder: Encoder[Edge] = Encoders.product[Edge]

  def extractPatterns(df: DataFrame): HashMap[Node, List[Edge]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    df.printSchema()  // This will print the schema so you can see all column names and types


    // Assuming 'id' corresponds to 'node_id' and constructing a hypothetical 'properties' map
    val nodesDF = df.select($"id".as[Long], map($"name", $"type").as[Map[String, String]]).as[Node]
    val edgesDF = df.select($"src_id", $"dst_id", map($"edge_properties").as[Map[String, String]]).as[Edge]  // Ensure 'src_id', 'dst_id', 'edge_properties' exist

    // Join nodes and edges DataFrames
    val patternsDF = nodesDF
      .join(edgesDF, $"id" === $"srcId" || $"id" === $"dstId", "left_outer")
      .groupBy("id", "properties")
      .agg(collect_list(struct($"srcId", $"dstId", $"properties")).as("edges"))

    // Collect results to the driver
    val patternMap = new HashMap[Node, List[Edge]]()
    patternsDF.collect().foreach { row =>
      val node = Node(row.getAs[Long]("id"), row.getAs[Map[String, String]]("properties"))
      val edges = row.getAs[Seq[Row]]("edges").map(e => Edge(e.getAs[Long]("srcId"), e.getAs[Long]("dstId"), e.getAs[Map[String, String]]("properties"))).toList
      patternMap.put(node, edges)
    }

    patternMap
  }
}
