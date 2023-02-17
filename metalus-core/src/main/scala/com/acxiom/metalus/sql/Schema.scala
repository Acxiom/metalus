package com.acxiom.metalus.sql

final case class Attribute(name: String, dataType: AttributeType, nullable: Option[Boolean], metadata: Option[Map[String, Any]]){

  private lazy val cleanName: String = {
    // return uppercase letters and digits only replacing everything else with an underscore
    val rawName = name.map(c => if (c.isLetterOrDigit) c.toUpper else '_')
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

  /**
   * cleans up a column name to a common case and removes characters that are not column name friendly
   *
   * @return a cleaned up version of the column name
   */
  def cleanColumnName: String = cleanName
}

final case class AttributeType(baseType: String, valueType: Option[AttributeType] = None, nameType: Option[AttributeType] = None, schema: Option[Schema] = None)

final case class Schema(attributes: Seq[Attribute])

