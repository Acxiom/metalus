package com.acxiom.metalus.spark

import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.sql.{Attribute, AttributeType, Schema, Transformations}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, Metadata, StructField, StructType}

package object sql {
  implicit class SchemaImplicits(schema: Schema) {
    def toStructType(transforms: Option[Transformations] = None): StructType = {
      StructType(schema.attributes.map(a => {
        if (transforms.flatMap(_.standardizeColumnNames).getOrElse(false)) {
          a.toStructField(transforms).copy(name = a.cleanColumnName)
        } else {
          a.toStructField(transforms)
        }
      }))
    }
  }

  implicit class StructTypeImplicits(struct: StructType) {
    def toSchema: Schema = Schema(struct.fields.map(_.toAttribute))
  }

  implicit class AttributeImplicits(attribute: Attribute) {
    def toStructField(transforms: Option[Transformations] = None): StructField = {
      StructField(attribute.name, attribute.dataType.toDataType(transforms), attribute.nullable.getOrElse(true), buildMetadata)
    }

    private def buildMetadata: Metadata = {
      if (attribute.metadata.getOrElse(Map()).isEmpty) Metadata.empty else Metadata.fromJson(JsonParser.serialize(attribute.metadata))
    }
  }

  implicit class StructFieldImplicits(field: StructField) {
    def toAttribute: Attribute = {
      Attribute(field.name, field.dataType.toAttributeType, None, None)
    }
  }

  implicit class AttributeTypeImplicits(attributeType: AttributeType) {
    def toDataType(transforms: Option[Transformations] = None): DataType = {
      attributeType.baseType.toLowerCase match {
        case "struct" => attributeType.schema.getOrElse(Schema(Seq())).toStructType(transforms)
        case "array" =>
          DataTypes.createArrayType(if (attributeType.valueType.isDefined) attributeType.valueType.get.toDataType() else DataTypes.StringType)
        case "map" =>
          DataTypes.createMapType(attributeType.nameType.getOrElse(AttributeType("string")).toDataType(),
            attributeType.valueType.getOrElse(AttributeType("string")).toDataType())
        case "string" => DataTypes.StringType
        case "double" => DataTypes.DoubleType
        case "integer" => DataTypes.IntegerType
        case "long" => DataTypes.LongType
        case "float" => DataTypes.FloatType
        case "boolean" => DataTypes.BooleanType
        case "timestamp" => DataTypes.TimestampType
        case "decimal" => DataTypes.createDecimalType()
        case _ => DataTypes.StringType
      }
    }
  }

  implicit class DataTypeImplicits(dataType: DataType) {
    def toAttributeType: AttributeType = {
      dataType.typeName match {
        case "struct" => AttributeType(dataType.typeName, schema = Some(dataType.asInstanceOf[StructType].toSchema))
        case "array" => AttributeType(dataType.typeName, valueType = Some(dataType.asInstanceOf[ArrayType].elementType.toAttributeType))
        case "map" =>
          val mapType = dataType.asInstanceOf[MapType]
          val keyType = AttributeType(mapType.keyType.typeName, Some(mapType.keyType.toAttributeType))
          val valueType = AttributeType(mapType.valueType.typeName, Some(mapType.valueType.toAttributeType))
          AttributeType(dataType.typeName, nameType = Some(keyType), valueType = Some(valueType))
        case _ => AttributeType(dataType.typeName)
      }
    }
  }
}
