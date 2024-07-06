import org.apache.spark.sql.{DataFrame, SparkSession, Encoder, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import scala.collection.mutable
import com.models.Pattern
import java.security.MessageDigest
import java.math.BigInteger


object DataToPattern {

  // def extractPatterns(df: DataFrame): List[Pattern] = {
  //   val patterns = scala.collection.mutable.ListBuffer[Pattern]()
    
  //   // Get the name of the first column as node label
  //   val nodeLabel = df.schema.fields.head.name
    
  //   df.collect().foreach { row =>
  //     // Use the node label (name of the first column)
  //     val label = nodeLabel
      
  //     // Extract properties: include column names only if the value is not null
  //     val properties = row.schema.fields.tail.filter { field =>
  //       row.getAs[Any](field.name) != null
  //     }.map(_.name).toSet
      
  //     // Extract edges based on columns that start with "edge_"
  //     val edges = row.schema.fields.tail.filter(_.name.startsWith("edge_")).map { field =>
  //       val edgeType = field.name.stripPrefix("edge_")
  //       val connectedNode = row.getAs[Any](field.name) match {
  //         case s: String => s
  //         case i: Int => i.toString
  //         case _ => throw new IllegalArgumentException("Unsupported data type for connected node")
  //       }
  //       (edgeType, connectedNode)
  //     }.toSet

  //      // Extract instance as a map of property names to values, filtering out null values
  //     val instance = row.schema.fields.tail.map { field =>
  //       field.name -> row.getAs[Any](field.name)
  //     }.filterNot { case (_, value) => value == null }.toMap
      
  //     val newPattern = Pattern(label, properties, edges, List(instance))

  //     // Add the new pattern or update the existing pattern with the new instance
  //     val existingPattern = patterns.find(p => p.nodeLabel == newPattern.nodeLabel && p.properties == newPattern.properties && p.edges == newPattern.edges)
  //     if (existingPattern.isDefined) {
  //       val updatedPattern = existingPattern.get.copy(instances = existingPattern.get.instances :+ instance)
  //       patterns -= existingPattern.get
  //       patterns += updatedPattern
  //     } else {
  //       patterns += newPattern
  //     }

  //   }
    
  //   patterns.toList
  // }

// def extractPatterns(df: DataFrame): mutable.HashMap[Pattern, List[Map[String, Any]]] = {
//   val patterns = new mutable.HashMap[Pattern, List[Map[String, Any]]]()

//   // Get the name of the first column as node label
//   val nodeLabel = df.schema.fields.head.name

//   df.collect().foreach { row =>
//     // Use the node label (name of the first column)
//     val label = nodeLabel

//     // Extract properties: include column names only if the value is not null
//     val properties = row.schema.fields.tail.filter { field =>
//       row.getAs[Any](field.name) != null
//     }.map(_.name).toSet

//     // Extract edges based on columns that start with "edge_"
//     val edges = row.schema.fields.tail.filter(_.name.startsWith("edge_")).map { field =>
//       val edgeType = field.name.stripPrefix("edge_")
//       val connectedNode = row.getAs[Any](field.name) match {
//         case s: String => s
//         case i: Int => i.toString
//         case _ => throw new IllegalArgumentException("Unsupported data type for connected node")
//       }
//       (edgeType, connectedNode)
//     }.toSet

//     // Extract instance as a map of property names to values, filtering out null values
//     val instance = row.schema.fields.map { field =>
//       field.name -> row.getAs[Any](field.name)
//     }.filterNot { case (_, value) => value == null }.toMap

//     val pattern = Pattern(label, properties, edges)

//     // Add the new instance to the corresponding pattern
//     patterns.updateWith(pattern) {
//       case Some(instances) => Some(instances :+ instance)
//       case None => Some(List(instance))
//     }
//   }

//   patterns
// }

def extractPatterns(df: DataFrame): mutable.HashMap[Pattern, List[Map[String, Any]]] = {
  val patterns = new mutable.HashMap[Pattern, List[Map[String, Any]]]()

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

    val pattern = Pattern(label, properties, edges)

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

  // def addPatternsAndRows(allPatterns: mutable.Set[Pattern], newPatterns: List[Pattern]): Unit = {
  //   newPatterns.foreach { newPattern =>
  //     val existingPattern = allPatterns.find(p => p.nodeLabel == newPattern.nodeLabel && p.properties == newPattern.properties && p.edges == newPattern.edges)
  //     existingPattern match {
  //       case Some(pattern) =>
  //         // Merge instances if the pattern already exists
  //         allPatterns.remove(pattern)
  //         // allPatterns.add(pattern.copy(instances = pattern.instances ++ newPattern.instances))
  //       case None =>
  //         allPatterns.add(newPattern)
  //     }
  //   }
  // }

  def addPatternsAndRows(allPatterns: mutable.HashMap[Pattern, List[Map[String, Any]]], newPatterns: mutable.HashMap[Pattern, List[Map[String, Any]]]): Unit = {
    newPatterns.foreach { case (newPattern, newInstances) =>
      allPatterns.updateWith(newPattern) {
        case Some(existingInstances) => Some(existingInstances ++ newInstances)
        case None => Some(newInstances)
      }
    }
  }

  def printSortedPatterns(allPatterns: mutable.HashMap[Pattern, List[Map[String, Any]]]): Unit = {
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


  def printPatternMap(patternMap: mutable.HashMap[Pattern, List[Map[String, Any]]]): Unit = {
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
