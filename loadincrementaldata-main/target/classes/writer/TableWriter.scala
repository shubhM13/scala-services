package writer

import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{LocationUtils, PrimaryKeyUtils, TableUtils}


class TableWriter(spark: SparkSession) {

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Writes data to Delta, either as a new table or merged into an existing table.
   *
   * @param df          Data to write
   * @param server      Origin server
   * @param db          Origin database
   * @param schema      Origin schema
   * @param table       Origin table
   * @param pks         Primary keys of table
   * @param forceReload Whether or not the table should be forced to reload, whether or not it already exists

   * @return            The written data
   */
  def write(df: DataFrame, server: String, db: String, schema: String, table: String, pks: Array[String], forceReload: Boolean): DataFrame = {
    spark.sql("CREATE CATALOG IF NOT EXISTS `" + TableUtils.getCatalogName(server) + "`")
    spark.sql("CREATE SCHEMA IF NOT EXISTS `" +TableUtils.getCatalogName(server) + "`.`" + TableUtils.getSchemaName(db) + "`")
    if (TableUtils.tableExists(server, db, schema, table, spark) && !forceReload) {
      logger.info(s"Data loader: Merging records for ${server}.${db}.${schema}.${table}")
      // Inserts, updates, and deletes can all be handled by a single merge statement. The merge will perform an insert if not matched,
      // or an update if matched. The deleted records have _deleted set to true, so a standard merge handles these.
      val records = df.where("SYS_CHANGE_OPERATION = 'I' OR SYS_CHANGE_OPERATION = 'U' OR SYS_CHANGE_OPERATION = 'D'")
        .drop("SYS_CHANGE_OPERATION")
        .drop("SYS_CHANGE_VERSION")

      if (records.count() > 0) {
        // See Databricks documentation for this switch: https://www.databricks.com/blog/2020/05/19/schema-evolution-in-merge-operations-and-operational-metrics-in-delta-lake.html
        spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

        val delta = DeltaTable.forPath(LocationUtils.locationOf(server, db, schema, table))
        delta.as("origin")
          .merge(
            records.as("i"),
            PrimaryKeyUtils.buildPrimaryKeyMatchStatement("origin", "i", pks))
          .whenMatched.updateAll()
          .whenNotMatched
          .insertAll()
          .execute()
      }
    } else {
      logger.info(s"Data loader: Writing new table for ${server}.${db}.${schema}.${table}")
      df.writeTo(TableUtils.getQualifiedTablePath(server, db, schema, table))
        .tableProperty("location", LocationUtils.locationOf(server, db, schema, table))
        .using("delta")
        .createOrReplace()
    }

    return df
  }
}
