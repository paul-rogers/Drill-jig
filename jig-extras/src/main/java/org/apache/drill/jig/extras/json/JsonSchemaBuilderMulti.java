package org.apache.drill.jig.extras.json;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.extras.json.JsonTupleSchema.JsonFieldSchema;
import org.apache.drill.jig.extras.json.reader.JsonScannerException;
import org.apache.drill.jig.extras.json.reader.JsonTupleReader;

/**
 * Sample n rows to infer a schema. Or, build a schema from a provided
 * schema definition. In either case, either leave the schema as a set
 * of nested structures, or flatten it to a single combined tuple.
 */
public class JsonSchemaBuilderMulti
{
  private JsonTupleReader reader;
  private int sampleSize;
  private boolean flattened;

  public JsonSchemaBuilderMulti( ) {
    
  }
  
  public JsonSchemaBuilderMulti withReader( JsonTupleReader reader ) {
    this.reader = reader;
    return this;
  }
  
  public JsonSchemaBuilderMulti withSampleSize( int n ) {
    sampleSize = n;
    return this;
  }
  
  public JsonSchemaBuilderMulti flattened( boolean select ) {
    flattened = select;
    return this;
  }
  
  public JsonTupleSchema build( ) {
    JsonValue value;
    int count = 0;
    JsonTupleSchema merged = null;
    while ( ( value = reader.next( ) ) != null ) {
      if ( value.getValueType() != ValueType.OBJECT ) {
        throw new JsonScannerException( "Object is required but " + value.getValueType() + " found." );
      }
      JsonTupleSchema schema = buildTuple( (JsonObject) value );
      if ( merged == null ) {
        merged = schema;
      }
      else {
        mergeSchema( merged, schema );
      }
      if ( ++count >= sampleSize )
        break;
    }
    if ( merged == null ) {
      return null; }
    if ( merged.count() == 0 ) {
      throw new JsonScannerException( "Empty schema" );
    }
    if ( flattened ) {
      merged = buildFlattened( merged );
    }
    return merged;
  }
  
  public JsonTupleSchema buildTuple( JsonObject seed )
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
      tupleSchema = buildTuple( (JsonObject) seed );
      break;
    default:
      type = inferScalarType( seed );
      cardinality = Cardinality.REPEATED;
      break;
    }
    return new JsonFieldSchema( name, type, cardinality, tupleSchema );
  }
  
  private void mergeSchema(JsonTupleSchema merged, JsonTupleSchema schema) {
    for ( FieldSchema newField : schema.fields( ) ) {
      String key = newField.name();
      JsonFieldSchema jsonField = (JsonFieldSchema) newField;
      JsonFieldSchema oldField = (JsonFieldSchema) schema.field( key );
      if ( oldField == null ) {
        schema.add( jsonField );
      }
      else {
        if ( ! mergeSchema( oldField, jsonField ) ) {
          throw new JsonScannerException( "Incompatible types for field " +
              key + ", type " + oldField.getDisplayType( ) +
              " and " + jsonField.getDisplayType() );
        }
      }
    }
  }
  
  private boolean mergeSchema(JsonFieldSchema oldField, JsonFieldSchema newField)
  {
    if ( oldField.type() == DataType.MAP || newField.type() == DataType.MAP ) {
      if ( oldField.type() != DataType.MAP  ||  newField.type() != DataType.MAP ) {
        return false;
      }
      mergeSchema( oldField.tupleSchema, newField.tupleSchema );
      return true;
    }
    if ( oldField.getCardinality() != newField.getCardinality() ) {
      return false;
    }
    if ( oldField.type( ) == newField.type( ) ) {
      return true;
    }
    if ( oldField.type().isScalar() && newField.type().isScalar() ) {
      oldField.becomeAny( );
      return true;
    }
    return false;
  }

  public JsonTupleSchema buildFlattened( JsonTupleSchema tuple ) {
    JsonTupleSchema flattened = new JsonTupleSchema( );
    buildFlattened( flattened, tuple, "" );
    return flattened;
  }

  public void buildFlattened( JsonTupleSchema flattened, JsonTupleSchema tuple, String path ) {
    for ( FieldSchema field : tuple.fields( ) ) {
      String name = path;
      if ( name.length() == 0 ) {
        name += "." + field.name();
      }
      if ( field.type() == DataType.MAP ) {
//        buildFlattened( flattened, (JsonTupleSchema) field.getStructure(), name );
      }
//      flattened.add( new JsonFieldSchema( name, field ) );
    }
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

  private static DataType inferScalarType( JsonValue seed )
  {
    switch ( seed.getValueType() ) {
    case TRUE:
    case FALSE:
      return DataType.BOOLEAN;
    case NULL:
      return DataType.VARIANT;
    case NUMBER:
      return DataType.DECIMAL;
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
    return type;
  }

}
