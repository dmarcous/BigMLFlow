trait FeaturesGenerator
{
  def generateFeatureMatrix( inputs : Map[String, RDD[String]]) : DataFrame
}
