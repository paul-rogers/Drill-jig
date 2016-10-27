package org.apache.drill.jig.extras.json.builder;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;

public class JsonBuilder {

  public static JsonObject build( TupleValue tuple ) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    TupleSchema schema = tuple.schema();
    for ( int i = 0;  i < schema.count();  i++ ) {
      buildValue( builder, schema.field( i ).name( ), tuple.field( i ) );
    }
    return builder.build();
  }

  private static void buildValue(JsonObjectBuilder builder, String name, FieldValue field) {
    switch ( field.type() ) {
    case BOOLEAN:
      builder.add( name, field.getBoolean() );
      break;
    case DECIMAL:
    case NUMBER:
      builder.add( name, field.getDecimal() );
      break;
    case FLOAT32:
    case FLOAT64:
      builder.add(name, field.getDouble() );
      break;
    case INT16:
    case INT32:
    case INT64:
    case INT8:
      builder.add( name, field.getLong( ) );
      break;
    case LIST:
      builder.add( name, buildArray( field.getArray() ) );
      break;
    case MAP:
      builder.add( name, buildMap( field.getMap() ) );
      break;
    case NULL:
      builder.addNull( name );
      break;
    case STRING:
      builder.add(name, field.getString() );
      break;
    case TUPLE:
      builder.add( name, build( field.getTuple() ) );
      break;
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case UNDEFINED:
    case UTC_DATE_TIME:
    case VARIANT:
    default:
      throw new IllegalStateException( "Unsupported type: " + field.type( ) );   
    }
  }

  private static JsonValue buildArray(ArrayValue array) {
    JsonArrayBuilder builder = Json.createArrayBuilder( );
    for ( int i = 0;  i < array.size();  i++ ) {
      buildValue( builder, array.get( i ) );
    }
    return builder.build( );
  }

  private static void buildValue(JsonArrayBuilder builder,
      FieldValue field) {
    switch ( field.type() ) {
    case BOOLEAN:
      builder.add( field.getBoolean() );
      break;
    case DECIMAL:
    case NUMBER:
      builder.add( field.getDecimal() );
      break;
    case FLOAT32:
    case FLOAT64:
      builder.add(field.getDouble() );
      break;
    case INT16:
    case INT32:
    case INT64:
    case INT8:
      builder.add( field.getLong( ) );
      break;
    case LIST:
      builder.add( buildArray( field.getArray() ) );
      break;
    case MAP:
      builder.add( buildMap( field.getMap() ) );
      break;
    case NULL:
      builder.addNull( );
      break;
    case STRING:
      builder.add(field.getString() );
      break;
    case TUPLE:
      builder.add(build( field.getTuple() ) );
      break;
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case UNDEFINED:
    case UTC_DATE_TIME:
    case VARIANT:
    default:
      throw new IllegalStateException( "Unsupported type: " + field.type( ) );   
    }
  }

  private static JsonValue buildMap(MapValue map) {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    for ( String key : map.keys() ) {
      buildValue(builder,  key, map.get( key ) );
    }
    return builder.build();
  }
}
