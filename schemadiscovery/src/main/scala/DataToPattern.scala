import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable
import java.security.MessageDigest
import java.math.BigInteger
import com.models.Pattern

object DataToPattern {

  def extractPatterns(df: DataFrame): mutable.LinkedHashMap[Pattern, List[Map[String, Any]]] = {
    val patterns = new mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]()

    // Get the name of the first column as node label
    val nodeLabel = df.schema.fields.head.name

    df.collect().foreach { row =>
      // Use the node label (name of the first column)
      val label = nodeLabel

      // Extract ID from the first column
      val idField = row.schema.fields.head
      val id = row.getAs[Any](idField.name).toString
      val hashedId = hashId(id)
      val uri = s"sdpg.gr/$label/$hashedId"

      // Extract properties: include column names only if the value is not null
      val properties = row.schema.fields.tail.filter { field =>
        row.getAs[Any](field.name) != null
      }.map(_.name).toSet

      // Extract edges based on columns that start with "edge_"
      val edges = row.schema.fields.tail.filter(_.name.startsWith("edge_")).map { field =>
        val edgeType = field.name.stripPrefix("edge_")
        val connectedNode = row.getAs[Any](field.name) match {
          case s: String => s
          case i: Int => i.toString
          case _ => throw new IllegalArgumentException("Unsupported data type for connected node")
        }
        (edgeType, connectedNode)
      }.toSet

      // Extract instance as a LinkedHashMap to maintain insertion order
      val instance = mutable.LinkedHashMap(idField.name -> id) ++ row.schema.fields.tail.map { field =>
        field.name -> row.getAs[Any](field.name)
      }.filterNot { case (_, value) => value == null } + ("uri" -> uri)

      // Determine if properties are optional
      val propertiesWithOptionality = properties.map(prop => prop -> true).toMap

      val pattern = Pattern(label, propertiesWithOptionality, edges)

      // Add the new instance to the corresponding pattern
      patterns.updateWith(pattern) {
        case Some(instances) => Some(instances :+ instance.toMap)
        case None => Some(List(instance.toMap))
      }
    }

    patterns
  }

  def hashId(id: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val digest = md.digest(id.getBytes)
    String.format("%064x", new BigInteger(1, digest))
  }

  def addPatternsAndRows(
    allPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]], 
    newPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]
  ): Unit = {
    newPatterns.foreach { case (newPattern, newInstances) =>
      allPatterns.updateWith(newPattern) {
        case Some(existingInstances) => Some(existingInstances ++ newInstances)
        case None => Some(newInstances)
      }
    }
  }

  def printSortedPatterns(allPatterns: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]): Unit = {
    // Print all unique patterns
    allPatterns.toList.sortBy { case (pattern, instances) =>
      (pattern.nodeLabel, pattern.properties.size, pattern.properties.mkString(", "))
    }.zipWithIndex.foreach { case ((pattern, instances), index) =>
      println(s"Unique Pattern ${index + 1}:")
      println(s"\tNode Label: ${pattern.nodeLabel}")
      println(s"\tProperties: ${pattern.properties.mkString(", ")}")
      println(s"\tEdges: ${pattern.edges.map(e => s"${e._1} -> ${e._2}").mkString(", ")}")
      println(s"\tNumber of Instances: ${instances.size}")
      instances.take(5).foreach { instance =>
        println(s"\t\tInstance: ${instance.map { case (k, v) => s"$k: $v" }.mkString(", ")}")
      }
      println()
    }
  }

  def printPatternMap(patternMap: mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]): Unit = {
    patternMap.zipWithIndex.foreach { case ((pattern, instances), index) =>
      println(s"Pattern ${index + 1}:")
      println(s"\tNode Label: ${pattern.nodeLabel}")
      println(s"\tProperties: ${pattern.properties.mkString(", ")}")
      println(s"\tEdges: ${pattern.edges.map(e => s"${e._1} -> ${e._2}").mkString(", ")}")
      println(s"\tNumber of Instances: ${instances.size}")
      instances.take(5).foreach { instance =>
        println(s"\t\tInstance: ${instance.map { case (k, v) => s"$k-> $v" }.mkString(", ")}")
      }
      println()
    }
  }
}
