package org.apache.drill.jig.extras.json;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.extras.json.JsonTupleSchema.JsonFieldSchema;

/**
 * Sample n rows to infer a schema. Or, build a schema from a provided
 * schema definition. In either case, either leave the schema as a set
 * of nested structures, or flatten it to a single combined tuple.
 */
public class JsonSchemaBuilder
{
  public JsonSchemaBuilder( )
  {
  }
  
  public JsonTupleSchema build( JsonObject seed )
  {
    JsonTupleSchema schema = new JsonTupleSchema( );
    for ( String key : seed.keySet() ) {
      JsonValue value = seed.get( key );
      schema.add( buildField( key, value ) );
    }
    return schema;
  }
  
  public JsonFieldSchema buildField( String name, JsonValue seed )
  {
    Cardinality cardinality;
    DataType type;
    JsonTupleSchema tupleSchema = null;
    
    switch ( seed.getValueType() ) {
    case ARRAY:
      type = inferArrayType( (JsonArray) seed );
      cardinality = Cardinality.REPEATED;
      break;
    case OBJECT:
      type = DataType.MAP;
      cardinality = Cardinality.OPTIONAL;
      tupleSchema = build( (JsonObject) seed );
      break;
    default:
      type = inferScalarType( seed );
      cardinality = Cardinality.OPTIONAL;
      break;
    }
    return new JsonFieldSchema( name, type, cardinality, tupleSchema );
  }
  
//  public void visitTuple( JsonTupleSchema schema, JsonObject seed )
//  {
//    for ( String key : seed.keySet() ) {
//      JsonValue value = seed.get( key );
//      JsonFieldSchema newField = visitValue( schema, key, value );
//      if ( newField == null ) {
//        continue;
//      }
//      JsonFieldSchema oldField = (JsonFieldSchema) schema.getField( key );
//      if ( oldField == null ) {
//        schema.add( newField );
//      }
//      else {
//        if ( ! mergeSchema( oldField, newField ) ) {
//          throw new JsonScannerException( "Incompatible types for field " +
//              key + ", type " + oldField.getTypeString( ) +
//              " and " + newField.getTypeString() );
//        }
//      }
//    }
//  }
//  
//  public JsonFieldSchema visitValue( JsonTupleSchema schema, String name, JsonValue seed )
//  {
//    FieldCardinality cardinality;
//    FieldType type;
//    JsonTupleSchema tupleSchema = null;
//    
//    switch ( seed.getValueType() ) {
//    case ARRAY:
//      type = inferArrayType( (JsonArray) seed );
//      cardinality = FieldCardinality.Repeated;
//      break;
//    case OBJECT:
//      type = FieldType.MAP;
//      cardinality = FieldCardinality.Optional;
//      tupleSchema = visitTupleField( schema, name, (JsonObject) seed );
//      break;
//    default:
//      type = inferScalarType( seed );
//      cardinality = FieldCardinality.Repeated;
//      break;
//    }
//    return new JsonFieldSchema( name, type, cardinality, tupleSchema );
//  }
//  
//  private JsonTupleSchema visitTupleField(JsonTupleSchema schema, String name, JsonObject seed) {
//    JsonFieldSchema schema = 
//  }

  static DataType inferScalarType( JsonValue seed )
  {
    switch ( seed.getValueType() ) {
    case TRUE:
    case FALSE:
      return DataType.BOOLEAN;
    case NULL:
      return DataType.VARIANT;
    case NUMBER:
      JsonNumber number = (JsonNumber) seed;
      if ( number.isIntegral() ) {
        return DataType.INT64;
      }
      else {
        return DataType.DECIMAL;
      }
    case STRING:
      return DataType.STRING;
    default:
      return DataType.VARIANT;
    }
  }

  private static DataType inferArrayType(JsonArray array) {
    DataType type = null;
    for ( JsonValue member : array ) {
      DataType memberType = inferScalarType( member );
      if ( type == null ) {
        type = memberType;
      }
      else if ( type != memberType ) {
        type = DataType.VARIANT;
      }
    }
    if ( type == null ) {
      type = DataType.VARIANT;
    }
    return type;
  }

}
