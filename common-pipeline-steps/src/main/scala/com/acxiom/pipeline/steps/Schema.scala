package com.acxiom.pipeline.steps

import com.acxiom.pipeline.steps.TransformationSteps.cleanColumnName
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

case class Attribute(name: String, dataType: String) {
  def toStructField: StructField = {
    val dataType = this.dataType.toLowerCase match {
      case "string" => DataTypes.StringType
      case "double" => DataTypes.DoubleType
      case "integer" => DataTypes.IntegerType
      case "timestamp" => DataTypes.TimestampType
      case _ => DataTypes.StringType
    }
    StructField(this.name, dataType)
  }
}

case class Schema(attributes: Seq[Attribute]) {
  def toStructType(transforms: Transformations = Transformations(List())): StructType = {
    StructType(attributes.map(a => {
      if (transforms.standardizeColumnNames.getOrElse(false)) {
        a.toStructField.copy(name = cleanColumnName(a.name))
      } else {
        a.toStructField
      }
    }))
  }
}

