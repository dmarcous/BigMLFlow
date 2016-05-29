object TrainingFeatureGeneratorApp
{

  def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("BigMLFlowCleanData").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlc = new org.apache.spark.sql.hive.HiveContext(sc)
    
        val udfs = new FavouriteClassificationUDFs( sqlc )
        val featuresGenerator : FeaturesGenerator = new TrainingFeaturesGenerator( udfs )
        val featureMatrix = featuresGenerator.generateFeatureMatrix(inputGenerator.inputs)
        writeFeatureMatrix( featureMatrix )
  }

  def writeFeatureMatrix( featureMatrix: DataFrame ) : Unit = 
  {
    val NUM_WRITE_PARTITIONS = 20
    featureMatrix.repartition(NUM_WRITE_PARTITIONS)
    .write.mode(SaveMode.Overwrite)
    .parquet("s3://my_training_feature_mat.parquet")
  }

}
