package com.acxiom.metalus.sql

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

object SchemaObj {
  def fromStructType(struct: StructType): Schema = {
    Schema(struct.fields.map(f => AttributeObj.fromStructField(f)))
  }
}

object AttributeObj {
  def fromStructField(field: StructField): Attribute = {
    Attribute(field.name, AttributeTypeObj.fromDataType(field.dataType), None, None)
  }
}

object AttributeTypeObj {
  def fromDataType(dataType: DataType): AttributeType = {
    dataType.typeName match {
      case "struct" => AttributeType(dataType.typeName, schema = Some(SchemaObj.fromStructType(dataType.asInstanceOf[StructType])))
      case "array" => AttributeType(dataType.typeName, valueType = Some(fromDataType(dataType.asInstanceOf[ArrayType].elementType)))
      case "map" => val mapType = dataType.asInstanceOf[MapType]
        val keyType = AttributeType(mapType.keyType.typeName, Some(fromDataType(mapType.keyType)))
        val valueType = AttributeType(mapType.valueType.typeName, Some(fromDataType(mapType.valueType)))
        AttributeType(dataType.typeName, nameType = Some(keyType), valueType = Some(valueType))
      case _ => AttributeType(dataType.typeName)
    }
  }
}
