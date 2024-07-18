import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

      // Extract properties: include column names only if the value is not null
      val properties = row.schema.fields.tail.filter { field =>
        row.getAs[Any](field.name) != null
      }.map(_.name).toSet

      // Extract edges based on columns that start with "edge_"
      val edges = row.schema.fields.tail.filter(_.name.startsWith("edge_")).map { field =>
        val edgeType = field.name.stripPrefix("edge_")
        val connectedNode = Option(row.getAs[Any](field.name)).map(_.toString).getOrElse("")
        (edgeType, connectedNode)
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
        // println(s"Matched header: ${field.name}")
        predefinedLabels.find(label => field.name.toLowerCase.contains(label.toLowerCase)).get
    }

    // If no match found in headers, check row values
    val matchedLabelInRow = matchedLabelInHeader.orElse {
      row.schema.fields.collectFirst {
        case field if predefinedLabels.exists(label => Option(row.getAs[Any](field.name)).exists(_.toString.toLowerCase.contains(label.toLowerCase))) =>
          // println(s"Matched field: ${field.name} with value: ${Option(row.getAs[Any](field.name)).map(_.toString).getOrElse("")}")
          predefinedLabels.find(label => Option(row.getAs[Any](field.name)).map(_.toString).getOrElse("").toLowerCase.contains(label.toLowerCase)).get
      }
    }

    // Debugging: print the entire row to understand why no label is being matched
    if (matchedLabelInRow.isEmpty) {
      println(s"No matching label found for row: ${row.mkString(", ")}")
    }

    // Return the matched label or "UnknownNodeLabel" if no match is found
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

  def extractPropertiesFromPattern(pattern: String, predefinedLabels: List[String]): Seq[String] = {
    val propertiesStart = pattern.indexOf("Map(") + 4 // +4 to skip "Map("
    val propertiesEnd = pattern.indexOf(')', propertiesStart)
    
    if (propertiesStart > -1 && propertiesEnd > propertiesStart) {
      val propertiesString = pattern.substring(propertiesStart, propertiesEnd)
      if (propertiesString.nonEmpty) {
        propertiesString.split(", ").map(_.split("->")(0).trim).toSeq
      } else {
        Seq.empty
      }
    } else {
      Seq.empty
    }
  }
}
