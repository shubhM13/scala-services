package reader

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TableUtils

import java.util.Optional

class DataLoader(spark: SparkSession, username:String, password:String) {

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Loads the data, either incrementally or fully.
   *
   * @param server      Origin server
   * @param db          Origin database
   * @param schema      Origin schema
   * @param table       Origin table
   * @param checkpoint  Latest checkpoint if exists, or empty optional if not.
   * @param forceReload Whether or not the table should be forced to reload, whether or not it already exists
   * @return            Loaded data, either full or incremental.
   */
  def load(server: String, db: String, schema:String, table: String, checkpoint: Optional[String], forceReload: Boolean): DataFrame = {
    if(TableUtils.tableExists(server, db, schema, table, spark) && !forceReload) {
      logger.info(s"Data loader: Running incremental load for ${server}.${db}.${schema}.${table}")
      return new IncrementalDataLoader(spark, username, password).load(server, db,schema, table, checkpoint)
    } else {
      logger.info(s"Data loader: Running initial load for ${server}.${db}.${schema}.${table}")
      return new InitialDataLoader(spark, username, password).load(server, db,schema, table)
    }
  }

  /**
   * Gets the latest version from the SQL server
   *
   * @param server  Origin server
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin table
   * @return        Latest version from the changetable
   */
  def getLatestVersion(server: String, db: String, schema: String, table: String): String = {
    logger.info(s"Data loader: Getting last version available from ${server}.${db}.${schema}.${table}")
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    val url = "jdbc:sqlserver://" + server + ".directs.com:1433;database=" + db
    val query = "(SELECT CHANGE_TRACKING_CURRENT_VERSION() AS ct) as tmp"
    val version = spark.read.jdbc(url, query, connectionProperties)
    if(version.count() > 0) {
      logger.info(s"Data loader: Found available version, ${version.head()(0).toString}")
      return version.head()(0).toString
    } else {
      logger.info(s"Data loader: Found nothing in changetable, assuming version 0")
      return "0" // Handle empty changetable
    }
  }

  /**
   * Gets the minimum valid version of the table.
   *
   * @param server  Origin server
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin table
   * @return        Minimum version from the changetable
   */
  def getMinVersion(server: String, db: String, schema: String, table: String): String = {
    logger.info(s"Data loader: Getting minimum version available from ${server}.${db}.${schema}.${table}")
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    val url = "jdbc:sqlserver://" + server + ".directs.com:1433;database=" + db
    val query = s"(SELECT CHANGE_TRACKING_MIN_VALID_VERSION ( OBJECT_ID('${schema}.${table}')) AS m) AS tmp"
    val version = spark.read.jdbc(url, query, connectionProperties)
    if(version.count() > 0) {
      logger.info(s"Data loader: Found minimum version, ${version.head()(0).toString}")
      return version.head()(0).toString
    } else {
      logger.info(s"Data loader: Found nothing in changetable, assuming minimum version 0")
      return "0" // Handle empty changetable
    }
  }
}
