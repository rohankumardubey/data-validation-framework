import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.io.{File,PrintWriter}
import org.apache.spark.sql.hive.HiveContext
import java.util._  
import java.io._
import scala.collection.mutable.Map


object LoadValidation {
  
  def main(args: Array[String]){
    
    
    val validationProp = new Properties()
    validationProp.load(new FileInputStream(Constants.VALIDATION_CONF_FILE_NAME))
        
    val confProp = new Properties()
    confProp.load(new FileInputStream(Constants.CONF_FILE_NAME))
   val url = confProp.getProperty("mysql") //"jdbc:mysql://localhost/test"
   val driver = "com.mysql.jdbc.Driver"
    val userName = confProp.getProperty("username")//"root"
    val password = confProp.getProperty("password") //"cloudera"
    val outputFileName = confProp.getProperty("outputFileName")
    val outputHeader = confProp.getProperty("outputHeader")
    
    var sMeasures  = Map[String, (String,String,String)]()
    var tMeasures  = Map[String, (String,String,String)]()
    
    var connection: java.sql.Connection = null
    
    var sourceCnt:Long = 0
    
    val masterName = "local[*]"
    val  appName = Constants.APP_NAME
    
    //Output parameters
    val sourceTable = validationProp.getProperty("validations.PaypalLoad.Source.Table") //"airspan_cdr"
    val targetTable = validationProp.getProperty("validations.PaypalLoad.Target.Table") //"employee"
    var resultStatus = Constants.VALIDATION_SUCCESS_STATUS
    var loadName = ""
    var loadType = ""
    var validationType = ""
    var validationEntity = ""
   var sc: SparkContext = null  
    var hiveContext: HiveContext = null
    try {
         connection = createMySqlConnection(url, userName, password)         
          val stmt = connection.createStatement() 
          
          
          val conf = new SparkConf().setAppName(appName).setMaster(masterName )
           println("in sparkcontext")
          sc = new SparkContext(conf)
          val hiveContext = createHiveContextObject(sc)
          println("in hivecontext")
          
          var key:String = ""
          var value: String = ""
          val  e = validationProp.propertyNames()
          val i = 0
          var cnt:Long = 0
          var queryStmt:String = ""
          var queryValue = ""
          //var queryIntFlag = true
           
          while (e.hasMoreElements()) {
            key = e.nextElement().toString()
            value = validationProp.getProperty(key)
                          
            val properties = key.split('.')
            if(properties.size == 5)
            {
               loadName  = properties(1)
               loadType  = properties(2)
               validationType = properties(3)
               validationEntity  = properties(4)
               queryStmt = "" 
               loadType match {
	            case "Source" => 
	              queryStmt  = buildMeasureQuery(validationType, validationEntity, value, sourceTable)
    
                if(queryStmt != ""){
                  if(validationType == "MIN" || validationType == "MAX"){
                    queryValue = executeScalarQuery(stmt, queryStmt, false);
                  }
                  else{
                    queryValue = executeScalarQuery(stmt, queryStmt);
                  }
                  sMeasures += (validationType -> (validationEntity, sourceTable, queryValue))
                }
	              
	             case "Target" => 
                 queryStmt  = buildMeasureQuery(validationType, validationEntity, value, targetTable)
	                
                 if(queryStmt != ""){
                   if(validationType == "MIN" || validationType == "MAX"){
                     queryValue = executeHiveScalaQuery(hiveContext, queryStmt, false)
                   }
                   else{
                     queryValue = executeHiveScalaQuery(hiveContext, queryStmt)
                   }
                     
                   tMeasures += (validationType -> (validationEntity, targetTable, queryValue))
                  }
                
                
	             case _ =>  println("Invalid LoadType %s", loadType)
              }
            }
          }
           
      } catch {
          case e: Exception => e.printStackTrace
          throw e
             
      }
      finally {
          connection.close
          sc.stop()
      }
      
    createOutputFile(outputFileName, outputHeader, loadName, sMeasures, tMeasures)
     
    //val targetQuery = "select count(*) from airspan_cdr"
  }
  
 def createOutputFile(fileName: String, fileHeader:String,  loadName: String, sourceMeasures: Map[String, (String,String,String)], targetMeasures: Map[String, (String,String,String)]   ) = {
   var sourceCnt = ""
   var targetCnt = ""
   var resultStatus = "Pass"
   var validationType = ""
   var entity = ""
   var sourceEntity = ""
   var targetEntity = ""
   var sourceTable = ""
   var targetTable = ""
   
   var outputResult = ""
  
   var outputFile = new java.io.PrintWriter(new File(fileName))
   println("File Created successfully")
   outputFile.write(fileHeader)
   outputFile.write("\n")
   
   for(smeasure <-  sourceMeasures){
     validationType = smeasure._1
     sourceEntity = smeasure._2._1
     sourceTable = smeasure._2._2
     sourceCnt = smeasure._2._3
     
     val tmeasure = targetMeasures.get(validationType)
     
     if (tmeasure != None){
       targetEntity = tmeasure.get._1
       targetTable = tmeasure.get._2
       targetCnt = tmeasure.get._3
     }
     
     if(sourceCnt.toInt == targetCnt.toInt)
     {
       resultStatus = "Pass"
     }
     else
     {
       resultStatus = "Fail"
     }
     
     outputResult = "%s,%s,%s,%s,%s,%s,%s,%s,%s".format(loadName,validationType, sourceEntity, sourceTable ,sourceCnt, targetTable, targetCnt, "0", resultStatus)
     //println(outputResult)
     outputFile.write(outputResult)  
   }
   
   outputFile.close() 
  }
  
 
  def buildMeasureQuery(validationType: String, validationEntity: String, value: String, tableName: String = ""): String = {
   var queryStmt = ""
   var validationName = ""
    if (validationEntity.contains("Column")){
         validationName = validationEntity.substring(0, validationEntity.length - 1)
    }
    else{
         validationName = validationEntity
    }
   validationType match {
     case "TotalCount" =>
         validationEntity match {
         case "Table" => queryStmt = Constants.TOTAL_COUNT_QUERY.format(tableName);
         case "Query" => queryStmt = value
         case _ => println("Invalid Valdiation Entity %s", validationEntity)
         }
     
     case "DistinctCount" =>  
       
       validationName match {
       case "Column" => queryStmt = Constants.DISTINCT_QUERY.format(value, tableName);
       case "Query" => queryStmt = value
       case _ => println("Invalid Valdiation Entity %s", validationEntity)
       }
     case "Min" =>
       
       validationName match {
       case "Column" => queryStmt = Constants.MIN_QUERY.format(value, tableName);
       case "Query" => queryStmt = value
       case _ => println("Invalid Valdiation Entity %s", validationEntity)
       }
       
     case "Max" =>
       validationName match {  
       case "Column" => queryStmt = Constants.MAX_QUERY.format(value, tableName);
       case "Query" => queryStmt = value
       case _ => println("Invalid Valdiation Entity %s", validationEntity)
       }
     case _ => println("Invalid validationEntity %s", validationEntity )
    }
   
    return queryStmt
 }
  
  def createMySqlConnection(dbUrl: String, userName: String, password: String): java.sql.Connection = {
     val conn = DriverManager.getConnection(dbUrl, userName, password)
     return conn
  }
  
  
  def executeScalarQuery(stmt:java.sql.Statement, query: String, returnFlag: Boolean = true ) :String = {
    val rs:ResultSet = stmt.executeQuery(query)
    
    var value = ""
    val  rsMetaData = rs.getMetaData
    var columnType = ""
    //var java.sql.ResultSetMetaData meta = 
    //ResultSetMetaData rsMetaData = rs.;
     //ResultSetMetaData rsMetaData = rs.get;
    while (rs.next) {
          columnType = rsMetaData.getColumnTypeName(1)
          if(returnFlag){
              value = rs.getInt(1).toString
                        }
          else{
          columnType.toLowerCase() match{
            case "int" =>   value = rs.getInt(1).toString
            case "varchar" => value = rs.getString(1)
            case "date" => value =   rs.getDate(1).toString
            case "double" => value = rs.getDouble(1).toString()
            case "float" => value =rs.getFloat(1).toString()
            case "Boolean" => value = rs.getBoolean(1).toString()
            case "Long" => value = rs.getLong(1).toString()
            
            }
          }
            
    }
    return value 
  }
  
 def executeHiveScalaQuery(hiveContext: HiveContext, query: String, returnFlag: Boolean = true):String = {
     val result = hiveContext.sql(query) 
     var value = ""
     var cnt: Long = 0
     if(returnFlag){
       cnt = result.first.getLong(0)
       value =  cnt.toString
     }
     else {
       value = result.first.getString(0)
     }
     return value
  }
  
  def createHiveContextObject(sparContext: SparkContext): org.apache.spark.sql.hive.HiveContext = {
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sparContext)
    return sqlContext
  }
}
  
  
  
