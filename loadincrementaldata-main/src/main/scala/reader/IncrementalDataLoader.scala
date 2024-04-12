package reader

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.PrimaryKeyUtils

import java.util.Optional

class IncrementalDataLoader(spark: SparkSession, username: String, password: String) {

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Loads incremental data from server, from the last known load.
   *
   * @param server      Origin server
   * @param db          Origin database
   * @param schema      Origin schema
   * @param table       Origin table
   * @param checkpoint  Last known checkpoint
   * @return            Loaded data from last checkpoint
   */
  def load(server: String, db: String, schema:String, table: String, checkpoint: Optional[String]): DataFrame = {
    if(!checkpoint.isPresent) {
      val err: String = s"Data loader: Incremental load was requested on a table ${server}.${db}.${schema}.${table}, but no checkpoint exists. Please re-load table."
      logger.info(err)
      throw new IllegalArgumentException(err)
    }

    val pks = PrimaryKeyUtils.getPrimaryKeyFromSqlServer(server, db, schema, table, username, password, spark)
    val join: String = PrimaryKeyUtils.buildPrimaryKeyMatchStatement("p", "CT", pks)
    val query: String = s"(SELECT p.*, CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION, 'false' as _deleted, Getdate() AS _last_modified_time FROM ${db}.${schema}.${table} p, CHANGETABLE(CHANGES ${db}.${schema}.${table}, ${checkpoint.get()}) AS CT WHERE ${join}) as tmp"
    logger.info(s"Data loader: Incremental records query executing on source, ${query}")

    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    val url = "jdbc:sqlserver://" + server + ".directs.com:1433;database=" + db
    val df = spark.read.jdbc(url, query, connectionProperties)
    logger.info(s"Data loader: Found non-deletes, count ${df.count().toString}")

    // Tricky point here - deletes will NOT show up in this table due to joining on the primary keys!
    // (because the primary keys are gone in the actual table)
    // Any record with a 'delete' here means that it was deleted then re-added, so it'll be ignored
    // by the writer. Deletes handled by the DeletedRecordsDataLoader
    return df
  }

}
