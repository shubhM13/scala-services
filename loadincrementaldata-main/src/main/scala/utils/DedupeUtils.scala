package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}

object DedupeUtils {

  /**
   * Removes duplicate records from an in-memory Dataframe
   *
   * @param df              Data to de-dupe
   * @param pks             Primary keys of data
   * @param priorityColumn  What column to use to determine which record to drop
   * @return                De-duplicated data
   */
  def dedupe(df: DataFrame, pks: Array[String], priorityColumn: String): DataFrame = {
    return df.coalesce(1).repartition(1).orderBy(desc(priorityColumn)).dropDuplicates(pks)
  }

}
