import org.apache.spark.sql.{DataFrame, SparkSession, Encoder, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable.HashMap
import scala.collection.mutable


object DataToPattern {

  def detectPatterns(df: DataFrame): Map[Seq[String], Seq[Seq[Any]]] = {
    val patternRows = mutable.Map[Seq[String], mutable.ListBuffer[Seq[Any]]]()

    df.collect().foreach { row =>
      val pattern = row.schema.fields.zipWithIndex.collect {
        case (field, index) if !row.isNullAt(index) => field.name
      }.toSeq

      if (!patternRows.contains(pattern)) {
        patternRows(pattern) = mutable.ListBuffer()
      }
      patternRows(pattern) += row.toSeq
    }

    patternRows.view.mapValues(_.toSeq).toMap
  }

  def addPatternsAndRows(allPatterns: mutable.Map[Seq[String], mutable.ListBuffer[Seq[Any]]], 
                         patterns: Map[Seq[String], Seq[Seq[Any]]]): Unit = {
    patterns.foreach { case (pattern, rows) =>
      if (!allPatterns.contains(pattern)) {
        allPatterns(pattern) = mutable.ListBuffer()
      }
      allPatterns(pattern) ++= rows
    }
  }

  def printSortedPatterns(allPatterns: mutable.Map[Seq[String], mutable.ListBuffer[Seq[Any]]]): Unit = {
    val sortedPatterns = allPatterns.keys.toSeq.sortBy(_.mkString(", "))

    sortedPatterns.zipWithIndex.foreach { case (pattern, index) =>
      println(s"Pattern ${index + 1}: ${pattern.mkString(", ")}")
      // Uncomment the following line if you also want to print the rows
      // println(s"Rows: ${allPatterns(pattern).map(_.mkString(", ")).mkString("[", "], [", "]")}")
    }
  }
}
