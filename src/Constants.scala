

object Constants {
  
  val VALIDATION_CONF_FILE_NAME = raw"conf/validations.conf"
  val CONF_FILE_NAME = raw"conf/validation-site.properties"
  val VALIDATION_SUCCESS_STATUS = "Pass"
  val VALIDATION_FAILURE_STATUS = "Fail"
  val DISTINCT_QUERY = "SELECT COUNT(DISTINCT %s) FROM %s"
  val TOTAL_COUNT_QUERY = "SELECT COUNT(*) FROM %s"
  val MAX_QUERY = "SELECT MAX(%s) FROM %s"
  val MIN_QUERY = "SELECT MIN(%s) FROM %s"
  val APP_NAME = "LoadValidation"

}
