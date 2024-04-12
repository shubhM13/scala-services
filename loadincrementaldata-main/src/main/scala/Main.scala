import constants.SchedulerConstants
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import reader.{DataLoader, DeletedRecordsDataLoader}
import services.{CheckpointService, SchedulerCheckpointService, VaultService}
import utils.{DedupeUtils, PrimaryKeyUtils, TableUtils}
import writer.TableWriter

import java.util.Optional

object Main {

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Gets the next tables to run for the scheduled job
   *
   * @return  Array of tables to run
   */
  def getScheduledRun(): Array[String] = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val checkpointService: SchedulerCheckpointService = new SchedulerCheckpointService(spark)
    var whichRun: Integer = 0
    val checkpoint = checkpointService.getCheckpoint()
    if(checkpoint.isPresent) {
      whichRun = checkpoint.get().toInt
    }
    if(whichRun >= SchedulerConstants.TABLE_GROUPS.length) {
      whichRun = 0
    }
    logger.info(s"Data loader: Running table group ${whichRun}, name ${SchedulerConstants.TABLE_GROUP_NAMES(whichRun)}")

    val runGroup = SchedulerConstants.TABLE_GROUPS(whichRun)
    checkpointService.saveCheckpoint((whichRun+1)+"")
    return runGroup
  }

  /**
   * Application entry point, called by Databricks job runner.
   * Verifies arguments are as expected, then
   * loops through all tables to load, and uses Spark to ingest them.
   *
   * @param args  Command line arguments from Databricks job run
   */
  def main(args: Array[String]): Unit = {
    if(SchedulerConstants.TABLE_GROUPS.length != SchedulerConstants.TABLE_GROUP_NAMES.length) {
      throw new RuntimeException("Misconfigured scheduler tables. Fix scheduler table list.")
    }

    var updatedArgs = args

    val spark: SparkSession = SparkSession.builder().getOrCreate()

    // Ensure all arguments have the same server
    if (args.length == 0) {
      logger.info("Data loader: Running via scheduler")
      updatedArgs = getScheduledRun()
    }
    val server = updatedArgs(0).split("\\.")(0)
    verifyArgumentIntegrity(updatedArgs, server)

    // Authenticate now so that we can share creds over tables.
    val vault: VaultService = new VaultService
    vault.authenticate()
    val creds: Map[String, String] = vault.getServerCredentials(server)

    val username = creds("username")
    val password = creds("password")

    for (arg <- updatedArgs) {
      loadTable(arg, spark, username, password, server)
    }
  }

  /**
   * Loads a single SQL Server table as a Delta table.
   *
   * @param argument  Single command-line argument from Databricks, to be split into parts.
   * @param spark     Spark session
   * @param username  Database username from Vault dynamic credentials
   * @param password  Database password from Vault dynamic credentials
   * @param server    Server name of all tables
   */
  def loadTable(argument: String, spark: SparkSession, username: String, password: String, server: String): Unit = {
    val startTime: Long = System.currentTimeMillis()
    logger.info(s"Data loader: Processing given table ${argument}")
    val parts: Array[String] = argument.split("\\.")

    val db: String = parts(1)
    val dbo: String = parts(2)
    val table: String = parts(3)

    val loader: DataLoader = new DataLoader(spark, username, password)
    val writer: TableWriter = new TableWriter(spark)
    val checkpointService: CheckpointService = new CheckpointService(spark)

    val currentVersion = loader.getLatestVersion(server, db, dbo, table)
    val minVersion = loader.getMinVersion(server, db, dbo, table)
    val checkpoint: Optional[String] = checkpointService.getCheckpoint(server, db, dbo, table)
    var forceReload: Boolean = false

    if(checkpoint.isPresent) {
      forceReload = minVersion.toInt > checkpoint.get().toInt &&
        minVersion.toInt > currentVersion.toInt &&
        currentVersion.toInt > 0 &&
        checkpoint.get().toInt > 0 // Omit 0 because it means empty changetable, no reset needed
    }

    if(forceReload) {
      logger.info(s"Data loader: WARNING Minimum version of ${minVersion} higher than current version of ${currentVersion} - table seems to have been purged, forcing reload of initial table.")
    }

    var df = loader.load(server, db, dbo, table, checkpoint, forceReload)
    val pks = PrimaryKeyUtils.getPrimaryKeyFromSqlServer(server, db, dbo, table, username, password, spark)
    if (checkpoint.isPresent && !forceReload) {
      var deletes = new DeletedRecordsDataLoader(spark, username, password).load(server, db, dbo, table, checkpoint)
      deletes = deletes.select(df.columns.head, df.columns.tail: _*)
      df = df.union(deletes)
      df = DedupeUtils.dedupe(df, pks, priorityColumn = "SYS_CHANGE_VERSION")
    }

    writer.write(df, server, db, dbo, table, pks, forceReload)
    checkpointService.saveCheckpoint(server, db, dbo, table, currentVersion)
    val time: Long = System.currentTimeMillis() - startTime
    val timeMinutes: Double = (((time.toDouble) / 1000) / 60)
    logger.info(s"Data loader: Processed record count = ${df.count().toString}")
    logger.info(s"Data loader: Completed ${argument} in ${time} ms (${timeMinutes} min)")
  }

  /**
   * Verifies that all arguments match the expected format (i.e. server.db.schema.table)
   * If arguments fail verification, exception will be thrown.
   *
   * @param args    Array of command-line arguments to verify
   * @param server  Expected server name
   */
  private def verifyArgumentIntegrity(args: Array[String], server: String): Unit = {
    for (arg <- args) {
      val parts: Array[String] = args(0).split("\\.")
      if (parts.length != 4) {
        val err: String = s"Data loader: Invalid argument " + arg + ". Must be in format <server>.<db>.<schema>.<table>"
        logger.info(err)
        throw new IllegalArgumentException(err)
      }
      if (!parts(0).equals(server)) {
        val err: String = s"Data loader: All tables should belong to the same server. Server is: " + server
        logger.info(err)
        throw new IllegalArgumentException(err)
      }
    }
  }
}
