object UsersParser 
{
    val KEY_VALUE_DELIMITER="\t"
    val EXPECTED_SPLIT_LENGTH=35
    
    val SQL_TIMESTAMP_DATE_PATTERN="yyyy-MM-dd HH:mm:ss.SSS"
       
    def parseUsers(line: String): Users =
    {
      try
      {
        val usersMap=splitUsersToFields(line)
        val usersObject=parseUsers(usersMap)
        return usersObject
      }
      catch
      {
        case e: Exception =>
          println(e)
          return createDefaultUsersRow()
      }
    }
}

object inputsGenerator
{
 
	def readUsers: DataFrame =
  {
      import sqlc.implicits._
      val rawUsersText = inputs.getOrElse( usersRDDName , null )
      val rawUsers : RDD[ Users ] = 
        StringToCaseClassRDDParser.parseUsersFile( rawUsersText )
      val users = rawUsers.toDF().as("users")
      users
  }
		   
  def parseUsersFile(dataRDD: RDD[String]): RDD[Users] = 
  {
      // Parse input lines to case class RDD
      val usersRDD=
        dataRDD.map{
        case(rawUsers)=>
              (UsersParser.parseUsers(rawUsers))
              
      usersRDD
  } 
}
