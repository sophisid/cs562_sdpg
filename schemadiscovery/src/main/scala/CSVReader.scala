import scala.io.Source

object CSVReader {
  def readCSV(filePath: String): (List[String], List[Map[String, String]]) = {
    val bufferedSource = Source.fromFile(filePath)
    val lines = bufferedSource.getLines()
    val headers = lines.next().split("\\|").map(_.trim).map(cleanHeader).toList
    val data = lines.map { line =>
      val cols = line.split("\\|").map(_.trim)
      headers.zip(cols).toMap
    }.toList
    bufferedSource.close()
    (headers, data)
  }

  def cleanHeader(header: String): String = {
    header.replaceAll("[:()]", "_").replaceAll("[^a-zA-Z0-9_]", "")
  }
}
