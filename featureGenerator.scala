trait FeaturesGenerator
{
  def generateFeatureMatrix( inputs : Map[String, RDD[String]]) : DataFrame
}

class TrainingFeaturesGenerator(udfs: FavouriteClassificationUDFs) extends FeaturesGenerator
{
    import udfs.sqlContext.implicits._
    override def generateFeatureMatrix( inputs : Map[String, RDD[String]]) : DataFrame =
      {
        val users = inputGenerator.readUsers(inputs)
    
        val filteredUsers = users.filter($"job" === "Wizard")
        val featureMatrix = 
              filteredUsers
              .withColumn("isHappy", ($"status" === "Happy").cast("int"))
              .withColumn("cleanName", udfs.extractName($"name"))
        return featureMatrix
      }
}
