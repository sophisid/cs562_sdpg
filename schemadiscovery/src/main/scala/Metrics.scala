// functions for computing all metrics
object Metrics {
  // Function to calculate precision for a single group
  def precision(ti: Set[String], gi: Set[String]): Double = {
    // println("ti:")
    // println(ti)
    // println("gi:")
    // println(gi)
    if (gi.isEmpty) 0.0
    else ti.intersect(gi).size.toDouble / gi.size.toDouble
  }

  // Function to calculate recall for a single group
  def recall(ti: Set[String], gi: Set[String]): Double = {
    if (ti.isEmpty) 0.0
    else ti.intersect(gi).size.toDouble / ti.size.toDouble
  }

  // // Function to calculate overall precision across multiple groups
  // def overallPrecision(tiList: List[Set[String]], giList: List[Set[String]]): Double = {
  //   val precisions = for ((ti, gi) <- tiList.zip(giList)) yield precision(ti, gi)
  //   precisions.sum / tiList.size
  // }

  // // Function to calculate overall recall across multiple groups
  // def overallRecall(tiList: List[Set[String]], giList: List[Set[String]]): Double = {
  //   val recalls = for ((ti, gi) <- tiList.zip(giList)) yield recall(ti, gi)
  //   recalls.sum / tiList.size
  // }

  // Function to calculate F1 score using overall precision and recall
  def f1Score(overallPrecision: Double, overallRecall: Double): Double = {
    if (overallPrecision + overallRecall == 0) 0.0
    else 2 * overallPrecision * overallRecall / (overallPrecision + overallRecall)
  }

    def overallPrecision(groundTruth: List[Set[String]], detectedPatterns: List[Set[String]]): Double = {
    val truePositives = detectedPatterns.count(groundTruth.contains)
    val falsePositives = detectedPatterns.size - truePositives
    if (detectedPatterns.isEmpty) 0.0 else truePositives.toDouble / (truePositives + falsePositives)
  }

  def overallRecall(groundTruth: List[Set[String]], detectedPatterns: List[Set[String]]): Double = {
    val truePositives = detectedPatterns.count(groundTruth.contains)
    val falseNegatives = groundTruth.size - truePositives
    if (groundTruth.isEmpty) 0.0 else truePositives.toDouble / (truePositives + falseNegatives)
  }


}