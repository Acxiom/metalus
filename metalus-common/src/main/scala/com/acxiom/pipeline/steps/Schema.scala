package com.acxiom.pipeline.steps

import com.acxiom.pipeline.steps.TransformationSteps.cleanColumnName
import org.apache.spark.sql.types._
import org.json4s.jackson.Serialization

case class Attribute(name: String, dataType: AttributeType, nullable: Boolean, metadata: Map[String, Any]) {
  def toStructField(transforms: Option[Transformations] = None): StructField = {
    StructField(this.name, dataType.toDataType(transforms), nullable, buildMetadata)
  }

  private def buildMetadata: Metadata = {
    if (metadata.isEmpty) Metadata.empty else Metadata.fromJson(Serialization.write(metadata)(org.json4s.DefaultFormats))
  }
}

object Attribute {
  def fromStructField(field: StructField): Attribute = {
    Attribute(field.name, AttributeType.fromDataType(field.dataType))
  }

  def apply(name: String, dataType: AttributeType): Attribute =
    new Attribute(name, dataType, true, Map())

  def apply(name: String, dataType: String): Attribute =
   new Attribute(name, AttributeType(dataType), true, Map())

  def apply(name: String, dataType: String, nullable: Boolean): Attribute =
    new Attribute(name, AttributeType(dataType), nullable, Map())

  def apply(name: String, dataType: AttributeType, nullable: Boolean): Attribute =
    Attribute(name, dataType, nullable, Map())
}

case class AttributeType(baseType: String, valueType: Option[AttributeType] = None, nameType: Option[AttributeType] = None, schema: Option[Schema] = None) {
  def toDataType(transforms: Option[Transformations] = None): DataType = {
    baseType.toLowerCase match {
      case "struct" => schema.getOrElse(Schema(Seq())).toStructType(transforms.getOrElse(Transformations(List())))
      case "array" =>
        DataTypes.createArrayType(if (valueType.isDefined) valueType.get.toDataType() else DataTypes.StringType)
      case "map" =>
        DataTypes.createMapType(nameType.getOrElse(AttributeType("string")).toDataType(), valueType.getOrElse(AttributeType("string")).toDataType())
      case "string" => DataTypes.StringType
      case "double" => DataTypes.DoubleType
      case "integer" => DataTypes.IntegerType
      case "timestamp" => DataTypes.TimestampType
      case "decimal" => DataTypes.createDecimalType()
      case _ => DataTypes.StringType
    }
  }
}

object AttributeType {
  def fromDataType(dataType: DataType): AttributeType = {
    dataType.typeName match {
      case "struct" => AttributeType(dataType.typeName, schema = Some(Schema.fromStructType(dataType.asInstanceOf[StructType])))
      case "array" => AttributeType(dataType.typeName, valueType = Some(fromDataType(dataType.asInstanceOf[ArrayType].elementType)))
      case "map" => val mapType = dataType.asInstanceOf[MapType]
        val keyType = AttributeType(mapType.keyType.typeName, Some(fromDataType(mapType.keyType)))
        val valueType = AttributeType(mapType.valueType.typeName, Some(fromDataType(mapType.valueType)))
        AttributeType(dataType.typeName, nameType=Some(keyType), valueType=Some(valueType))
      case _ => AttributeType(dataType.typeName)
    }
  }
}

case class Schema(attributes: Seq[Attribute]) {
  def toStructType(transforms: Transformations = Transformations(List())): StructType = {
    StructType(attributes.map(a => {
      if (transforms.standardizeColumnNames.getOrElse(false)) {
        a.toStructField().copy(name = cleanColumnName(a.name))
      } else {
        a.toStructField()
      }
    }))
  }
}

object Schema {
  def fromStructType(struct: StructType): Schema = {
    Schema(struct.fields.map(f => Attribute.fromStructField(f)))
  }
}
