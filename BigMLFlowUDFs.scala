// SQL UDFs
class BigMLFlowUDFs( val sqlContext: SQLContext ) extends Serializable
{
  val extractName = 
    sqlContext.udf.register("extractName", 
        (name: String) => 
          name.trim.toUpperCase)
}
