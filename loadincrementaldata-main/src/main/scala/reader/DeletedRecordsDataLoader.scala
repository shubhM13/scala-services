package reader

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{PrimaryKeyUtils, TableUtils}

import java.util.Optional

class DeletedRecordsDataLoader(spark: SparkSession, username: String, password: String) {

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Loads deleted records from table, from last known checkpoint
   *
   * @param server        Origin server
   * @param db            Origin database
   * @param schema        Origin schema
   * @param table         Origin table
   * @param checkpoint    Last known checkpoint
   * @return              Incremental delete data
   */
  def load(server: String, db: String, schema:String, table: String, checkpoint: Optional[String]): DataFrame = {
    if(!checkpoint.isPresent) {
      val err: String = s"Data loader: Incremental load was requested on a table ${server}.${db}.${schema}.${table}, but no checkpoint exists. Please re-load table."
      logger.info(err)
      throw new IllegalArgumentException(err)
    }

    val pks = PrimaryKeyUtils.getPrimaryKeyFromSqlServer(server, db, schema, table, username, password, spark)
    val cols = PrimaryKeyUtils.buildDeletedColsFromPk("CT", pks)
    val query: String = s"(SELECT ${cols} FROM CHANGETABLE(CHANGES ${db}.${schema}.${table}, ${checkpoint.get()}) AS CT WHERE CT.SYS_CHANGE_OPERATION = 'D') as tmp"
    logger.info(s"Data loader: Deleted records query executing on source, ${query}")

    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    val url = "jdbc:sqlserver://" + server + ".directs.com:1433;database=" + db
    val df = spark.read.jdbc(url, query, connectionProperties)

    df.createOrReplaceTempView("deleteValues")

    val originWithoutTimestamp = spark.sql(s"SELECT * FROM ${TableUtils.getCatalogName(server)}.${TableUtils.getSchemaName(db)}.${TableUtils.getTableName(schema, table)}").drop("_last_modified_time")
    originWithoutTimestamp.createOrReplaceTempView("originWithoutTime")
    val join: String = PrimaryKeyUtils.buildPrimaryKeyMatchStatement("origin", "d", pks)
    val mergedDeletedQuery = s"SELECT origin.*, d.SYS_CHANGE_OPERATION, d.SYS_CHANGE_VERSION, d._last_modified_time FROM originWithoutTime origin, deleteValues d WHERE ${join}"
    val mergedDeletes = spark.sql(mergedDeletedQuery).drop("_deleted").withColumn("_deleted", lit("true"))
    logger.info(s"Data loader: Found deletes, count ${mergedDeletes.count().toString}")

    return mergedDeletes
  }

}
