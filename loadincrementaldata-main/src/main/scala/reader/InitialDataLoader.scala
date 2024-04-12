package reader

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class InitialDataLoader(spark: SparkSession, username: String, password: String) {

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Loads initial data from table
   *
   * @param server  Origin server
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin schema
   * @return        All data from table
   */
  def load(server: String, db: String, schema:String, table: String): DataFrame = {
    val query = s"(SELECT t.*, 'false' AS _deleted, Getdate() AS _last_modified_time FROM ${db}.${schema}.${table} t) AS tmp"
    logger.info(s"Data loader: Initial load query executing on source, ${query}")
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    val url = "jdbc:sqlserver://" + server + ".directs.com:1433;database=" + db
    val df = spark.read.option("numPartitions", 2).jdbc(url, query, connectionProperties)
    logger.info(s"Data loader: Found initial records, count ${df.count().toString}")
    return df
  }

}
