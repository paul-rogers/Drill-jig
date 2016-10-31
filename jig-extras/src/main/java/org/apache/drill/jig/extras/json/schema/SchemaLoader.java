package org.apache.drill.jig.extras.json.schema;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.extras.json.schema.JsonSchema.*;

public class SchemaLoader {

  public static final String DEFINITIONS_KW = "definitions";
  public static final String REF_KW = "$ref";
  public static final String TYPE_KW = "type";
  public static final String PROPERTIES_KW = "properties";
  public static final String REQUIRED_KW = "required";
  public static final String NULLABLE_KW = "nullable";
  public static final String FORMAT_KW = "format";
  public static final String MINIMUM_KW = "minimum";
  public static final String MAXIMUM_KW = "maximum";
  public static final String ADDITIONAL_PROPERTIES_KW = "additionalProperties";
  public static final String ITEMS_KW = "items";
  public static final String JIG_TYPE_KW = "jigType";
  public static final String NAME_KW = "name";
  public static final String ENUM_KW = "enum";
  public static final String ADDITIONAL_ITEMS_KW = "additionalItems";
  
  public JsonSchema load( JsonObject schema ) {
    return parseRoot( schema );
  }

  private JsonSchema parseRoot(JsonObject schema) {
    JsonSchema schemaDef = new JsonSchema( );
    parseDefinitions( schemaDef, getObject( schema, DEFINITIONS_KW ) );
    schemaDef.root = parseValueBody( schema );
    return schemaDef;
  }
  
  private void parseDefinitions(JsonSchema schemaDef, JsonObject defns) {
    if ( defns == null )
      return;
    for ( String defnName : defns.keySet() ) {
      JsonObject defnBody = getObject( defns, defnName );
      ValueSchema defn = parseValueBody( defnBody );
      defn.name = defnName;
      schemaDef.definitions.add( defn );
    }
  }

  private ValueSchema parseValueBody( JsonObject value) {
    ValueSchema valueSchema = new ValueSchema( );
    if ( value.containsKey( REF_KW ) ) {
      valueSchema.ref = parseObjectRef( getString( value, REF_KW ) );
    } else {
      String type = getString( value, TYPE_KW );
      if ( type == null )
        warning( "Type keyword missing for an object schema" );
      valueSchema.jigType = toJigType( getString( value, JIG_TYPE_KW ) );
      valueSchema.nullable = getBoolean( value, NULLABLE_KW, valueSchema.nullable );
      valueSchema.type = toJsonType( getString( value, TYPE_KW ) );
      valueSchema.typeSchema = parseType( valueSchema.type, value );
    }
    return valueSchema;
  }

  private String parseObjectRef(String string) {
    assert false;
    return null;
  }

  private TypeSchema parseType(JsonType type, JsonObject valueDef) {
    switch( type ) {
    case NUMBER: {
      NumberSchema schema = new NumberSchema( );
      schema.minimum = getNumber( valueDef, MINIMUM_KW );
      schema.maximum = getNumber( valueDef, MAXIMUM_KW );
      return schema;
    }
    case ARRAY: {
      JsonValue items = valueDef.get( ITEMS_KW );
      if ( items == null ) {
        return new ArraySchema( );
      } else if ( items.getValueType() == ValueType.ARRAY ) {
        return parseTuple( (JsonArray) items );
      } else if ( items.getValueType() == ValueType.OBJECT ) {
        ArraySchema schema = new ArraySchema( );
        schema.elementSchema = parseValueBody( (JsonObject) valueDef );
        return schema;
      }
    }
    case OBJECT: {
      ObjectSchema schema = new ObjectSchema( );
      parseProperties( schema, getObject( valueDef, PROPERTIES_KW ) );
      parseRequired( schema, getArray( valueDef, REQUIRED_KW ) );
      schema.additionalProperties = getBoolean( valueDef, ADDITIONAL_PROPERTIES_KW, schema.additionalProperties );
      return schema;
    }
    case STRING: {
      StringSchema schema = new StringSchema( );
      schema.format = getString( valueDef, FORMAT_KW );
      return schema;
    }
    default:
      return null;    
    }
  }

  private TupleSchema parseTuple(JsonArray fields) {
    TupleSchema schema = new TupleSchema( );
    for ( JsonValue element : fields ) {
      JsonObject elementDef = toObject( element, "tuple element" );
      ValueSchema elementSchema = parseValueBody( elementDef );
      elementSchema.name = getString( elementDef, NAME_KW );
      schema.addProperty( elementSchema );
    }
    return schema;
  }

  private void parseProperties(ObjectSchema schema, JsonObject valueDef) {
    for ( String propName : valueDef.keySet( ) ) {
      ValueSchema propSchema = parseValueBody( toObject( valueDef, propName ) );
      propSchema.name = propName;
      schema.addProperty( propSchema );
    }
  }

  private void parseRequired(ObjectSchema schema, JsonArray array) {
    if ( array == null )
      return;
    for ( JsonValue value : array ) {
      String propName = toString( value, REQUIRED_KW );
      ValueSchema propSchema = schema.getProperty( propName );
      if ( propSchema == null ) {
        warning( "Required property " + propName + " is not defined." );
      } else {
        propSchema.required = true;
      }
    }
  }

  private static Map<String,JsonSchema.JsonType> jsonTypeMap = buildJsonTypeMap( );
  
  private static Map<String, JsonSchema.JsonType> buildJsonTypeMap() {
    Map<String,JsonSchema.JsonType> map = new HashMap<>( );
    map.put( "null", JsonSchema.JsonType.NULL );
    map.put( "number", JsonSchema.JsonType.NUMBER );
    map.put( "string", JsonSchema.JsonType.STRING );
    map.put( "boolean", JsonSchema.JsonType.BOOLEAN );
    map.put( "object", JsonSchema.JsonType.OBJECT );
    map.put( "array", JsonSchema.JsonType.ARRAY );
    return map;
  }

  private JsonSchema.JsonType toJsonType(String type) {
    JsonSchema.JsonType jsonType = jsonTypeMap.get( type );
    if ( jsonType == null )
      throw new InvalidSchemaException( "Unknown JSON type: " + type );
    return jsonType;
  }

  private static Map<String,DataType> jigTypeMap = buildJigTypeMap( );
  
  private static Map<String, DataType> buildJigTypeMap() {
    Map<String,DataType> map = new HashMap<>( );
    map.put( "null", DataType.NULL );
    map.put( "byte", DataType.INT8 );
    map.put( "short", DataType.INT16 );
    map.put( "integer", DataType.INT32 );
    map.put( "int", DataType.INT32 );
    map.put( "long", DataType.INT64 );
    map.put( "float", DataType.FLOAT32 );
    map.put( "double", DataType.FLOAT64 );
    map.put( "decimal", DataType.DECIMAL );
    map.put( "number", DataType.NUMBER );
    map.put( "string", DataType.STRING );
    map.put( "boolean", DataType.BOOLEAN );
    map.put( "object", DataType.MAP );
    map.put( "array", DataType.LIST );
    map.put( "variant", DataType.VARIANT );
    return map;
  }

  private DataType toJigType(String type) {
    DataType jigType = jigTypeMap.get( type );
    if ( jigType == null )
      throw new InvalidSchemaException( "Unknown Jig type: " + type );
    return jigType;
  }

  private void warning(String msg) {
    System.err.println( msg );
  }

  private JsonObject getObject(JsonObject obj, String key) {
    JsonValue value = obj.get( key );
    if ( value == null )
      return null;
    return toObject( value, "key " + key );
  }

  private JsonObject toObject(JsonValue value, String context) {
    try {
      return (JsonObject) value;
    } catch (ClassCastException e) {
      throw new InvalidSchemaException( "Expected object for " + context );
    }
  }

  private JsonArray getArray(JsonObject obj, String key) {
    JsonValue value = obj.get( key );
    if ( value == null )
      return null;
    return toArray( value, "key " + key );
  }

  private JsonArray toArray(JsonValue value, String context) {
    try {
      return (JsonArray) value;
    } catch (ClassCastException e) {
      throw new InvalidSchemaException( "Expected array for " + context );
    }
  }

  private String getString(JsonObject obj, String key) {
    JsonValue value = obj.get( key );
    if ( value == null )
      return null;
    return toString( value, "key " + key );
  }

  private String toString(JsonValue value, String context) {
    try {
      return ((JsonString) value).getString();
    } catch (ClassCastException e) {
      throw new InvalidSchemaException( "Expected string for " + context );
    }
  }

  private boolean getBoolean(JsonObject obj, String key, boolean defaultValue) {
    Boolean value = getBoolean( obj, key );
    if ( value == null )
      return defaultValue;
    return value;
  }

  private Boolean getBoolean(JsonObject obj, String key) {
    JsonValue value = obj.get( key );
    if ( value == null )
      return null;
    return toBoolean( obj.get( key ), "key " + key );
  }

  private boolean toBoolean(JsonValue value, String context) {
    if ( value == JsonValue.TRUE )
      return true;
    if ( value == JsonValue.FALSE )
      return false;
    throw new InvalidSchemaException( "Expected boolean for " + context );
  }
  
  private BigDecimal getNumber(JsonObject obj, String key) {
    JsonValue value = obj.get( key );
    if ( value == null )
      return null;
    return toNumber( value, "key " + key );
  }

  private BigDecimal toNumber(JsonValue value, String context) {
    try {
      return ((JsonNumber) value).bigDecimalValue();
    } catch (ClassCastException e) {
      throw new InvalidSchemaException( "Expected number for " + context );
    }
  }
}
