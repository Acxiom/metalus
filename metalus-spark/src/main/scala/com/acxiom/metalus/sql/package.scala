package com.acxiom.metalus

import com.acxiom.metalus.parser.JsonParser
import org.apache.spark.sql.types._
package object sql {
  implicit class SchemaImplicits(schema: Schema) {
    def toStructType(transforms: Transformations = Transformations(List())): StructType = {
      StructType(schema.attributes.map(a => {
        if (transforms.standardizeColumnNames.getOrElse(false)) {
          a.toStructField().copy(name = schema.cleanColumnName(a.name))
        } else {
          a.toStructField()
        }
      }))
    }
  }

  implicit class AttributeImplicits(attribute: Attribute) {
    def toStructField(transforms: Option[Transformations] = None): StructField = {
      StructField(attribute.name, attribute.dataType.toDataType(transforms), attribute.nullable.getOrElse(true), buildMetadata)
    }

    private def buildMetadata: Metadata = {
      if (attribute.metadata.getOrElse(Map()).isEmpty) Metadata.empty else Metadata.fromJson(JsonParser.serialize(attribute.metadata))
    }
  }

  implicit class AttributeTypeImplicits(attributeType: AttributeType) {
    def toDataType(transforms: Option[Transformations] = None): DataType = {
      attributeType.baseType.toLowerCase match {
        case "struct" => attributeType.schema.getOrElse(Schema(Seq())).toStructType(transforms.getOrElse(Transformations(List())))
        case "array" =>
          DataTypes.createArrayType(if (attributeType.valueType.isDefined) attributeType.valueType.get.toDataType() else DataTypes.StringType)
        case "map" =>
          DataTypes.createMapType(attributeType.nameType.getOrElse(AttributeType("string")).toDataType(), attributeType.valueType.getOrElse(AttributeType("string")).toDataType())
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

    //    def fromDataType(dataType: DataType): AttributeType = {
    //      dataType.typeName match {
    //        case "struct" => AttributeType(dataType.typeName, schema = Some(Schema.fromStructType(dataType.asInstanceOf[StructType])))
    //        case "array" => AttributeType(dataType.typeName, valueType = Some(fromDataType(dataType.asInstanceOf[ArrayType].elementType)))
    //        case "map" => val mapType = dataType.asInstanceOf[MapType]
    //          val keyType = AttributeType(mapType.keyType.typeName, Some(fromDataType(mapType.keyType)))
    //          val valueType = AttributeType(mapType.valueType.typeName, Some(fromDataType(mapType.valueType)))
    //          AttributeType(dataType.typeName, nameType = Some(keyType), valueType = Some(valueType))
    //        case _ => AttributeType(dataType.typeName)
    //      }
    //    }
  }
}
