// Clustering.scala
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.feature.{VectorAssembler, BucketedRandomProjectionLSH}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object Clustering {
  // Function to dynamically get numeric columns
  def getNumericColumns(df: DataFrame): Array[String] = {
    df.schema.fields
      .filter(field => field.dataType == DoubleType || field.dataType == IntegerType)  // You can add more numeric types if needed
      .map(_.name)
  }

  // Gaussian Mixture Model for clustering
  def applyGaussianMixture(df: DataFrame): DataFrame = {
    // Dynamically find numeric columns
    val numericColumns = getNumericColumns(df)

    // Check if there are numeric columns to work with
    if (numericColumns.isEmpty) {
      throw new IllegalArgumentException("No numeric columns found in the DataFrame")
    }

    // Assemble feature columns into a single feature vector
    val assembler = new VectorAssembler()
      .setInputCols(numericColumns)  // Dynamically set feature columns
      .setOutputCol("features")

    val featureDF = assembler.transform(df)

    // Apply Gaussian Mixture Model (GMM) for clustering
    val gmm = new GaussianMixture()
      .setK(3)  // Set number of clusters
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    // Fit the model and transform the DataFrame
    val predictions = gmm.fit(featureDF).transform(featureDF)

    predictions
  }

  // LSH for approximate similarity search
  def applyLSH(df: DataFrame): DataFrame = {
    // Ensure the "features" column exists after applying Gaussian Mixture
    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setInputCol("features")  // Use the features column
      .setOutputCol("hashes")

    // Fit the LSH model and transform the DataFrame
    val lshModel = lsh.fit(df)
    val transformedDF = lshModel.transform(df)

    transformedDF
  }
}
