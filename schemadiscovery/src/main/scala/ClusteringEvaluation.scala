import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType

object ClusteringEvaluation {

  def computeMetricsWithoutPairwise(evaluationDF: DataFrame): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._

    // Determine majority type for each cluster
    val clusterTypeCountsDF = evaluationDF
      .groupBy("predictedClusterLabel", "_labels")
      .count()

    val windowSpec = Window.partitionBy("predictedClusterLabel").orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select($"predictedClusterLabel", $"_labels".as("majorityType"))

    // Attach majority type to each node
    val evaluationWithMajorityDF = evaluationDF
      .join(majorityTypeDF, "predictedClusterLabel")

    // Compute True Positives and False Positives
    val TP = evaluationWithMajorityDF
      .filter($"_labels" === $"majorityType")
      .count()

    val FP = evaluationWithMajorityDF
      .filter($"_labels" =!= $"majorityType")
      .count()

    // Compute False Negatives
    val totalActualPositivesDF = evaluationDF
      .groupBy("_labels")
      .count()
      .withColumnRenamed("count", "totalActual")

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy("majorityType")
      .count()
      .withColumnRenamed("count", "totalPredicted")

    val FN = totalActualPositivesDF.join(
      totalPredictedPositivesDF,
      totalActualPositivesDF("_labels") === totalPredictedPositivesDF("majorityType"),
      "left_outer"
    )
      .withColumn("FN", $"totalActual" - coalesce($"totalPredicted", lit(0)))
      .agg(sum("FN"))
      .collect()(0).getLong(0)

    // Calculate Precision, Recall, F1-Score
    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(f"Precision: $precision%.4f")
    println(f"Recall: $recall%.4f")
    println(f"F1-Score: $f1Score%.4f")
  }
}
