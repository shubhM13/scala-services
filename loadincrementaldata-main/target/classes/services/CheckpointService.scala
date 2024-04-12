package services

import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import utils.TableUtils

import java.util.Optional

class CheckpointService(spark: SparkSession) {

  val logger: Logger = Logger.getLogger(this.getClass)
  val bucket: String = System.getenv("BUCKET")

  /**
   * Gets the checkpoint from the Delta checkpoint tracking database
   *
   * @param server      Origin server
   * @param db          Origin database
   * @param schema      Origin schema
   * @param table       Origin table
   * @return            Latest checkpoint for the sql server table
   */
  def getCheckpoint(server: String, db: String, schema:String, table: String): Optional[String] = {
    logger.info(s"Data loader: Attempting to find checkpoint for ${server}.${db}.${schema}.${table}")
    spark.sql("CREATE CATALOG IF NOT EXISTS `internal`")
    spark.sql("CREATE SCHEMA IF NOT EXISTS `data_platform`")
    if(spark.catalog.tableExists("internal.data_platform.checkpoints")) {
      val df = spark.sql("SELECT value FROM internal.data_platform.checkpoints WHERE server = '" + server + "' AND db = '" + db + "' AND dbo = '" + schema + "' AND table = '" + table + "'")
      if(df.count() == 1) {
        val found = df.head().getString(0)
        logger.info(s"Data loader: Found checkpoint for ${server}.${db}.${schema}.${table}, = ${found}")
        return Optional.of(found)
      } else if(df.count() > 1) {
        val err = s"Data loader: Invalid checkpoint state for ${server}.${db}.${schema}.${table} - more than one checkpoint exists"
        logger.info(err)
        throw new RuntimeException(err)
      } else {
        logger.info(s"Data loader: No checkpoint for ${server}.${db}.${schema}.${table} - should be initial load")
        return Optional.empty()
      }
    }
    logger.info(s"Data loader: No checkpoint table exists - should be initial load")
    // no table exists yet
    return Optional.empty()
  }

  /**
   * Saves a checkpoint as the latest for the table, in the Delta checkpoint tracking database
   *
   * @param server      Origin server
   * @param db          Origin database
   * @param schema      Origin schema
   * @param table       Origin table
   * @param checkpoint  Checkpoint to save
   */
  def saveCheckpoint(server: String, db: String, schema:String, table: String, checkpoint: String): Unit = {
    logger.info(s"Data loader: Saving ${checkpoint} as checkpoint for ${server}.${db}.${schema}.${table}")
    spark.sql("CREATE CATALOG IF NOT EXISTS `internal`")
    spark.sql("CREATE SCHEMA IF NOT EXISTS `internal`.`data_platform`")
    if(spark.catalog.tableExists("internal.data_platform.checkpoints")) {

      logger.info(s"Data loader: Merging checkpoint with existing")
      val columns = Seq("server","db","dbo","table","value")
      val data = Seq((server, db, schema, table, checkpoint))
      val rdd = spark.sparkContext.parallelize(data)
      val df = spark.createDataFrame(rdd).toDF(columns:_*)

      val delta = DeltaTable.forPath(s"s3://${bucket}/catalog/unity/internal/data_platform/checkpoints")
      delta.as("origin")
        .merge(
          df.as("i"),
          "origin.server = i.server AND origin.db = i.db AND origin.dbo = i.dbo AND origin.table = i.table")
        .whenMatched.updateAll()
        .whenNotMatched
        .insertAll()
        .execute()
    } else {
      logger.info(s"Data loader: Creating new checkpoint table")
      // make the checkpoint df
      val columns = Seq("server","db","dbo","table","value")
      val data = Seq((server, db,schema, table, checkpoint))
      val rdd = spark.sparkContext.parallelize(data)
      val df = spark.createDataFrame(rdd).toDF(columns:_*)

      df.writeTo("internal.data_platform.checkpoints")
        .tableProperty("location", s"s3://${bucket}/catalog/unity/internal/data_platform/checkpoints")
        .using("delta")
        .createOrReplace()
    }
  }
}
