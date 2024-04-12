package services

import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.util.Optional

class SchedulerCheckpointService(spark: SparkSession) {

  val logger: Logger = Logger.getLogger(this.getClass)
  val bucket: String = System.getenv("BUCKET")

  /**
   * Gets the checkpoint from the Delta checkpoint tracking database
   *
   * @return            Latest checkpoint for the sql server table
   */
  def getCheckpoint(): Optional[String] = {
    logger.info(s"Data loader: Finding scheduler checkpoint")
    if(spark.catalog.tableExists("internal.data_platform.scheduler_checkpoints")) {
      val df = spark.sql("SELECT value FROM internal.data_platform.scheduler_checkpoints WHERE id = 'run'")
      if(df.count() == 1) {
        val found = df.head().getString(0)
        logger.info(s"Data loader: Scheduler checkpoint found, ${found}")
        return Optional.of(found)
      } else if(df.count() > 1) {
        val err = s"Data loader: More than one checkpoint exists for scheduler"
        logger.info(err)
        throw new RuntimeException(err)
      } else {
        logger.info(s"Data loader: No scheduler checkpoint found")
        return Optional.empty()
      }
    }
    logger.info(s"Data loader: No scheduler checkpoint table found")
    // no table exists yet
    return Optional.empty()
  }

  /**
   * Saves a checkpoint as the latest for the table, in the Delta checkpoint tracking database
   *
   * @param checkpoint  Checkpoint to save
   */
  def saveCheckpoint(checkpoint: String): Unit = {
    logger.info(s"Data loader: Saving ${checkpoint} as checkpoint for scheduler")
    spark.sql("CREATE CATALOG IF NOT EXISTS `internal`")
    spark.sql("CREATE SCHEMA IF NOT EXISTS `internal`.`data_platform`")
    if(spark.catalog.tableExists("internal.data_platform.scheduler_checkpoints")) {

      logger.info(s"Data loader: Merging scheduler checkpoint with existing")
      val columns = Seq("id","value")
      val data = Seq(("run",checkpoint))
      val rdd = spark.sparkContext.parallelize(data)
      val df = spark.createDataFrame(rdd).toDF(columns:_*)

      val delta = DeltaTable.forPath(s"s3://${bucket}/catalog/unity/internal/data_platform/scheduler_checkpoints")
      delta.as("origin")
        .merge(
          df.as("i"),
          "origin.id = i.id")
        .whenMatched.updateAll()
        .whenNotMatched
        .insertAll()
        .execute()
    } else {
      logger.info(s"Data loader: Creating new scheduler checkpoint table")
      // make the checkpoint df
      val columns = Seq("id","value")
      val data = Seq(("run",checkpoint))
      val rdd = spark.sparkContext.parallelize(data)
      val df = spark.createDataFrame(rdd).toDF(columns:_*)

      df.writeTo("internal.data_platform.scheduler_checkpoints")
        .tableProperty("location", s"s3://${bucket}/catalog/unity/internal/data_platform/scheduler_checkpoints")
        .using("delta")
        .createOrReplace()
    }
  }
}
