package org.apache.drill.jig.extras.json.schema;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.drill.jig.api.DataType;

public class JsonSchema {

  public enum JsonType {
    NULL, BOOLEAN, NUMBER, INTEGER, STRING, OBJECT, ARRAY, VARIANT;

    public String jsonName() {
      if ( this == VARIANT ) {
        return "[ \"null\", \"boolean\", \"number\", \"string\" ]";
      } else {
        return name( ).toLowerCase();
      }
    }
  }
  
  public abstract static class TypeSchema {

    public void toJson(JsonObjectBuilder builder) {
    }   
  }
  
  public static class NumberSchema extends TypeSchema {
    public BigDecimal minimum;
    public BigDecimal maximum;
    
    @Override
    public void toJson(JsonObjectBuilder builder) {
      if ( minimum != null ) {
        builder.add( SchemaLoader.MINIMUM_KW, minimum );
      }   
      if ( maximum != null ) {
        builder.add( SchemaLoader.MAXIMUM_KW, minimum );
      }   
    }
  }
  
  public static class StringSchema extends TypeSchema {
    public String format;
    public List<String> enumValues;
    
    @Override
    public void toJson(JsonObjectBuilder builder) {
      if ( format != null ) {
        builder.add( SchemaLoader.FORMAT_KW, format );
      }
      if ( enumValues != null  &&  ! enumValues.isEmpty() ) {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for ( String value : enumValues ) {
          arrayBuilder.add( value );
        }
        builder.add( SchemaLoader.ENUM_KW, arrayBuilder.build( ) );
      }
    }
  }
  
  public static abstract class AbstractTupleSchema extends TypeSchema {
    List<ValueSchema> properties = new ArrayList<>( );
    Map<String,ValueSchema> propertyKeys = new HashMap<>( );
    
    public ValueSchema getProperty( String name ) {
      return propertyKeys.get( name );
    }

    public void addProperty(ValueSchema property) {
      assert property.name != null;
      if ( propertyKeys.containsKey( property.name ) )
        throw new InvalidSchemaException( "Duplicate property: " + property.name );
      propertyKeys.put( property.name, property );
      properties.add( property );
    }
  }
  
  public static class ObjectSchema extends AbstractTupleSchema {
    public boolean additionalProperties;
    
    @Override
    public void toJson(JsonObjectBuilder builder) {
      JsonObjectBuilder propsBuilder = Json.createObjectBuilder();
      boolean hasRequired = false;
      for ( ValueSchema prop : properties ) {
        propsBuilder.add( prop.name, prop.toJson( ) );
        hasRequired |= prop.required;
      }
      builder.add( SchemaLoader.PROPERTIES_KW, propsBuilder.build( ) );
      if ( hasRequired ) {
        JsonArrayBuilder reqBuilder = Json.createArrayBuilder();
        for ( ValueSchema prop : properties ) {
          if ( prop.required ) {
            reqBuilder.add( prop.name );
          }
        }
        builder.add( SchemaLoader.REQUIRED_KW, reqBuilder.build( ) );
      }
      builder.add( SchemaLoader.ADDITIONAL_PROPERTIES_KW, additionalProperties );
    }
  }
  
  public static class ArraySchema extends TypeSchema {
    public ValueSchema elementSchema;
    
    @Override
    public void toJson(JsonObjectBuilder builder) {
      if ( elementSchema != null ) {
        builder.add( SchemaLoader.ITEMS_KW, elementSchema.toJson() );
      }
    }
  }
  
  public static class TupleSchema extends AbstractTupleSchema {
    public boolean additionalItems;
    
    @Override
    public void toJson(JsonObjectBuilder builder) {
      JsonArrayBuilder itemsBuilder = Json.createArrayBuilder();
      for ( ValueSchema prop : properties ) {
        JsonObjectBuilder elementBuilder = Json.createObjectBuilder();
        elementBuilder.add( SchemaLoader.NAME_KW, prop.name );
        prop.toJson(elementBuilder);
        itemsBuilder.add( elementBuilder.build( ) );
      }
      builder.add( SchemaLoader.ITEMS_KW, itemsBuilder.build( ) );
      builder.add( SchemaLoader.ADDITIONAL_ITEMS_KW, additionalItems );
    }
  }
  
  public static class ValueSchema {
    
    public String name;
    public boolean required;
    public boolean hasNullable;
    public boolean nullable;
    public JsonType type;
    public DataType jigType;
    public TypeSchema typeSchema;
    public String ref;
    
    public JsonObject toJson( ) {
      JsonObjectBuilder builder = Json.createObjectBuilder();
      toJson( builder );
      return builder.build();
    }

    private void toJson(JsonObjectBuilder builder) {
      JsonArrayBuilder typeBuilder = Json.createArrayBuilder();
      if ( hasNullable  &&  nullable ) {
        typeBuilder.add( JsonType.NULL.jsonName() );
      }
      if ( type == JsonType.VARIANT ) {
        typeBuilder.add( JsonType.BOOLEAN.jsonName() );
        typeBuilder.add( JsonType.NUMBER.jsonName() );
        typeBuilder.add( JsonType.STRING.jsonName() );
      } else {
        typeBuilder.add( type.jsonName() );
      }
      JsonArray types = typeBuilder.build();
      if ( types.size() == 1 )
        builder.add( SchemaLoader.TYPE_KW, types.get( 0 ) );
      else
        builder.add( SchemaLoader.TYPE_KW, types );
//      if ( hasNullable ) {
//        builder.add( SchemaLoader.NULLABLE_KW, nullable );
//      }
      if ( jigType != null ) {
        builder.add( SchemaLoader.JIG_TYPE_KW, jigType.name().toLowerCase() );
      }
      if ( typeSchema != null )
        typeSchema.toJson( builder );
    }
  }

  public ValueSchema root;
  public List<ValueSchema> definitions = new ArrayList<>( );
  
  public JsonObject toJson() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    root.toJson( builder );
    if ( ! definitions.isEmpty() ) {
      JsonObjectBuilder defBuilder = Json.createObjectBuilder();
      for ( ValueSchema defn : definitions ) {
        defBuilder.add( defn.name, defn.toJson( ) );
      }
      builder.add( SchemaLoader.DEFINITIONS_KW, defBuilder.build( ) );
    }
    return builder.build();
  }
}
