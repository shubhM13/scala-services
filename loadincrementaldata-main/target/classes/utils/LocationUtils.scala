package utils

object LocationUtils {

  val BUCKET = System.getenv("BUCKET")

  /**
   * Gets the Delta table location for a replicated SQL Server
   *
   * @param server  Origin server
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin table
   * @return        S3 Location of table
   */
  def locationOf(server: String, db: String, schema: String, table: String): String = {
    return s"s3://${BUCKET}/catalog/unity/data_platform/tables/" +
      TableUtils.getCatalogName(server) + "/" + TableUtils.getSchemaName(db) + "/" + TableUtils.getTableName(schema, table)
  }

}
