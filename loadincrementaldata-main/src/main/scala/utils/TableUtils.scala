package utils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Helper to handle tables within Delta lake
 */
object TableUtils {

  /**
   * Determines whether the table exists in Delta
   *
   * @param server  Origin server
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin table
   * @param spark   Spark session
   * @return        True if table exists, false if not.
   */
  def tableExists(server: String, db: String, schema: String, table:String, spark: SparkSession): Boolean = {
    val fullName: String = getQualifiedTablePath(server, db, schema, table);
    return spark.catalog.tableExists(fullName)
  }

  /**
   * Gets the full Unity Catalog path of the table, as a String
   *
   * @param server  Origin server
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin table
   * @return        Fully qualified Unity path name
   */
  def getQualifiedTablePath(server: String, db: String, schema: String, table:String): String = {
    return getCatalogName(server) + "." + getSchemaName(db) + "." + getTableName(schema, table)
  }

  /**
   * Finds what the table name should be in Delta based on original names,
   * excluding the server
   *
   * @param db      Origin database
   * @param schema  Origin schema
   * @param table   Origin table
   * @return        Table name in Unity
   */
  def getTableName(schema: String, table:String): String = {
    val parsedDbo = NameUtils.replaceDisallowedChars(schema)
    val parsedTable = NameUtils.replaceDisallowedChars(table)

    return parsedDbo + "_" + parsedTable
  }

  /**
   * Gets the Unity Catalog schema name for database
   *
   * @param db  Origin database
   * @return    Schema name in Unity
   */
  def getSchemaName(db: String): String = {
    return NameUtils.replaceDisallowedChars(db)
  }

  /**
   * Gets Unity Catalog catalog name based on original server name
   *
   * @param server  Origin server
   * @return        Catalog name in Unity
   */
  def getCatalogName(server: String): String = {
    return "raw_" + NameUtils.replaceDisallowedChars(server)
  }

}
