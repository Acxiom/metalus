package com.acxiom.metalus.sql

case class Attribute(name: String, dataType: AttributeType, nullable: Option[Boolean], metadata: Option[Map[String, Any]])

case class AttributeType(baseType: String, valueType: Option[AttributeType] = None, nameType: Option[AttributeType] = None, schema: Option[Schema] = None)

case class Schema(attributes: Seq[Attribute]) {
  /**
   * cleans up a column name to a common case and removes characters that are not column name friendly
   *
   * @param name the column name that needs to be cleaned up
   * @return a cleaned up version of the column name
   */
  private[sql] def cleanColumnName(name: String): String = {
    // return uppercase letters and digits only replacing everything else with an underscore
    val rawName = name.map(c => {
      if (c.isLetterOrDigit) c.toUpper else '_'
    })
      .replaceAll("_+", "_")
      .stripPrefix("_")
      .stripSuffix("_") // cleanup any extra _

    // if it starts with a digit, add the 'c_' prefix
    if (rawName(0).isDigit) {
      "C_" + rawName
    } else {
      rawName
    }
  }
}

