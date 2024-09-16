import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, VectorAssembler, StringIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.GaussianMixture


object Clustering {

  def createFeatureVectors(df: DataFrame): DataFrame = {
    val stringColumns = df.schema.fields.filter(_.dataType == org.apache.spark.sql.types.StringType).map(_.name)
    val indexers = stringColumns.map { col =>
      new StringIndexer()
        .setInputCol(col)
        .setOutputCol(s"${col}_index")
        .setHandleInvalid("keep")  // Skip rows with invalid (NULL) values
    }

    // Apply all string indexers using a Pipeline
    val pipeline = new Pipeline().setStages(indexers)
    val indexedDF = pipeline.fit(df).transform(df)

    // Collect all columns for features (numeric and indexed columns)
    val featureColumns = indexedDF.columns.filter(_.endsWith("_index")) ++ df.columns.filterNot(stringColumns.contains(_))

    // VectorAssembler to combine the indexed columns into a feature vector
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    // Transform the dataset to feature vectors
    assembler.transform(indexedDF)
  }

  // Refine LSH clusters using GMM (Gaussian Mixture Model) clustering
  def refineClusters(df: DataFrame): DataFrame = {
    val gmm = new GaussianMixture()
      .setK(5) // You can adjust the number of clusters (k)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = gmm.fit(df)
    val clusteredDF = model.transform(df)

    clusteredDF
  }

    def performLSHClustering(df: DataFrame): DataFrame = {
      val lsh = new BucketedRandomProjectionLSH()
        .setInputCol("features")
        .setOutputCol("hashes")
        .setBucketLength(2.0) // Adjust bucket length as needed

      val model = lsh.fit(df)
      val lshDF = model.transform(df)

      lshDF
    }
}

