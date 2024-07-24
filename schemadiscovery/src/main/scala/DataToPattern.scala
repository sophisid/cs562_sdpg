import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.mutable
import java.security.MessageDigest
import java.math.BigInteger
import com.models.Pattern

object DataToPattern {

  val predefinedLabels = List("Person", "Place", "Event", "Organization", "Location", "Product", "Company", "City", "Country")

  def extractPatterns(df: DataFrame): mutable.LinkedHashMap[Pattern, List[Map[String, Any]]] = {
    val patterns = new mutable.LinkedHashMap[Pattern, List[Map[String, Any]]]()

    df.collect().foreach { row =>
      val label = extractNodeLabel(row, predefinedLabels)

      // Assuming the first column is the id
      val idField = row.schema.fields.head
      val id = Option(row.getAs[Any](idField.name)).map(_.toString).getOrElse("")
      val hashedId = hashId(id)
      val uri = s"sdpg.gr/$label/$hashedId"

      // Extract properties: include column names only if the value is not null and it's not an edge
      val properties = row.schema.fields.tail.filter { field =>
        !predefinedLabels.exists(label => field.name.toLowerCase.contains(label.toLowerCase)) && row.getAs[Any](field.name) != null
      }.map(_.name).toSet

      // Extract edges based on columns that match predefined labels
      val edges = row.schema.fields.tail.filter { field =>
        predefinedLabels.exists(label => field.name.toLowerCase.contains(label.toLowerCase))
      }.map { field =>
        field.name
      }.toSet

      // Extract instance as a LinkedHashMap to maintain insertion order
      val instance = mutable.LinkedHashMap(idField.name -> id) ++ row.schema.fields.tail.map { field =>
        field.name -> row.getAs[Any](field.name)
      }.filterNot { case (_, value) => value == null } + ("uri" -> uri)

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

  def extractNodeLabel(row: Row, predefinedLabels: List[String]): String = {
    // Check if any of the header names contain predefined labels
    val matchedLabelInHeader = row.schema.fields.collectFirst {
      case field if predefinedLabels.exists(label => field.name.toLowerCase.contains(label.toLowerCase)) =>
        predefinedLabels.find(label => field.name.toLowerCase.contains(label.toLowerCase)).get
    }

    // If no match found in headers, check row values
    val matchedLabelInRow = matchedLabelInHeader.orElse {
      row.schema.fields.collectFirst {
        case field if predefinedLabels.exists(label => Option(row.getAs[Any](field.name)).exists(_.toString.toLowerCase.contains(label.toLowerCase))) =>
          predefinedLabels.find(label => Option(row.getAs[Any](field.name)).map(_.toString).getOrElse("").toLowerCase.contains(label.toLowerCase)).get
      }
    }

    if (matchedLabelInRow.isEmpty) {
      println(s"No matching label found for row: ${row.mkString(", ")}")
    }

    matchedLabelInRow.getOrElse("UnknownNodeLabel")
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
    allPatterns.toList.sortBy { case (pattern, instances) =>
      (pattern.nodeLabel, pattern.properties.size, pattern.properties.mkString(", "))
    }.zipWithIndex.foreach { case ((pattern, instances), index) =>
      println(s"Unique Pattern ${index + 1}:")
      println(s"\tNode Label: ${pattern.nodeLabel}")
      println(s"\tProperties: ${pattern.properties.mkString(", ")}")
      println(s"\tEdges: ${pattern.edges.mkString(", ")}")
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
      println(s"\tEdges: ${pattern.edges.mkString(", ")}")
      println(s"\tNumber of Instances: ${instances.size}")
      instances.take(5).foreach { instance =>
        println(s"\t\tInstance: ${instance.map { case (k, v) => s"$k-> $v" }.mkString(", ")}")
      }
      println()
    }
  }

def extractPropertiesFromPattern(pattern: String): Seq[String] = {
  val mapStart = pattern.indexOf("HashMap(") match {
    case -1 => pattern.indexOf("Map(")
    case idx => idx
  }
  if (mapStart == -1) return Seq.empty // Ensure starting delimiter exists

  val mapEnd = pattern.indexOf(')', mapStart)
  if (mapEnd == -1 || mapEnd <= mapStart) return Seq.empty // Ensure ending delimiter exists and is after start

  try {
    val propertiesString = pattern.substring(mapStart + 7, mapEnd) // Adjust start index based on delimiter
    if (propertiesString.nonEmpty) {
      propertiesString.split(", ").map(_.split("->")(0).trim).toSeq
    } else {
      Seq.empty
    }
  } catch {
    case e: Exception => 
      // println(s"Error extracting properties from pattern: $pattern. Error: ${e.getMessage}")
      Seq.empty
  }
}


  def extractEdgesFromPattern(pattern: String): Set[String] = {
    val setStart = pattern.indexOf("Set(")
    val setEnd = pattern.indexOf(')', setStart)
    if (setStart == -1 || setEnd <= setStart) return Set.empty

    val edgesString = pattern.substring(setStart + 4, setEnd) // +4 to skip "Set("
    if (edgesString.nonEmpty) {
      edgesString.split(", ").map(_.stripPrefix("(").stripSuffix(")").split("->")(0).trim).toSet
    } else {
      Set.empty
    }
  }
}
