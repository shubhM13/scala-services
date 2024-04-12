package utils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.util.Properties

/**
 * Helper to manage primary key of tables
 */
object PrimaryKeyUtils {

  /**
   * Queries original SQL Server to find the primary key of table.
   *
   * @param server    Origin server
   * @param db        Origin database
   * @param schema    Origin schema
   * @param table     Origin table
   * @param username  Database username from Vault dynamic credentials
   * @param password  Database password from Vault dynamic credentials
   * @param spark     Spark session
   * @return          Primary keys, as a String array.
   */
  def getPrimaryKeyFromSqlServer(server: String, db: String, schema: String, table: String, username: String, password: String, spark: SparkSession): Array[String] = {
    val query = s"(SELECT COLUMN_NAME FROM ${db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = '${table}' AND TABLE_SCHEMA = '${schema}') AS tmp"
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    val url = "jdbc:sqlserver://" + server + ".directs.com:1433;database=" + db
    val df = spark.read.jdbc(url, query, connectionProperties)
    return df.collect.map(row=>row.getString(0))
  }

  /**
   * Creates a WHERE statement based on the primary key, for use in finding deletes.
   *
   * @param tableName Alias for original table
   * @param pks       Primary keys of table
   * @return          Deleted records match statement
   */
  def buildDeletedColsFromPk(tableName: String, pks: Array[String]): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(s"${tableName}.SYS_CHANGE_OPERATION, ${tableName}.SYS_CHANGE_VERSION, ")
    for(pk <- pks) {
      sb.append(s"${tableName}.${pk}, ")
    }
    sb.append("'true' as _deleted, GetDate() as _last_modified_time")
    return sb.toString()
  }

  /**
   * Creates a WHERE statement to match records based on primary key, for use in
   * the Delta merge statement
   *
   * @param originTable Alias for original table
   * @param newTable    Alias for new table
   * @param pks         Primary keys of table
   * @return            Merge match statement
   */
  def buildPrimaryKeyMatchStatement(originTable: String, newTable: String, pks: Array[String]): String = {
    val sb: StringBuilder = new StringBuilder()
    for(pk <- pks) {
      sb.append(originTable + "." + pk + " = " + newTable + "." + pk + " AND ")
    }
    if(pks.length >= 1) {
      sb.setLength(sb.length - " AND ".length)
    }
    return sb.toString()
  }

}
