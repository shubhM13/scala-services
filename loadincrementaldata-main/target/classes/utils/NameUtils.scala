package utils

object NameUtils {

  /**
   * Cleans up a name to satisfy metastore requirements
   *
   * @param original  Original table/server/etc. name
   * @return          Sanitized name to work for Delta
   */
  def replaceDisallowedChars(original: String): String = {
    return original.replace("-", "_").replace(" ", "_")
  }

}
