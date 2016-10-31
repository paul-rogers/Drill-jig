package org.apache.drill.jig.extras.json.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.FieldValueFactory;

/**
 * Parses a JSON object into a Jig schema, inferring Jig fields and types from
 * the JSON fields.
 */

public class ObjectParser {
  
  public enum JsonNodeType { OBJECT, ARRAY, SCALAR };
  
  /**
   * Represents a logical description of one aspect of a schema inferred
   * from a set of JSON nodes. Focuses on the JSON structure itself.
   */
  
  public abstract class JsonSchemaNode {
    public DataType type;   
    public String name;
    public boolean nullable = false;
    
    public JsonSchemaNode( DataType type ) {
      this.type = type;
      nullable = type == DataType.NULL;
    }
    
    public JsonSchemaNode merge(JsonSchemaNode other, FieldValueFactory factory) {
      if ( other.type == DataType.UNDEFINED )
        return this;
      if ( this.type == DataType.UNDEFINED )
        return other;
      if ( other.type == DataType.NULL ) {
        this.nullable = true;
        return this;
      }
      if ( this.type == DataType.NULL ) {
        other.nullable = true;
        return other;
      }
      if ( this.getClass() != other.getClass() ) {
        throw new ValueConversionError( "Incompatible types: " + type  + " and " +other.type );
      }
      JsonSchemaNode result = doMerge( other, factory );
      result.nullable |= other.nullable;
      return result;                       
    }

    protected abstract JsonSchemaNode doMerge(JsonSchemaNode other, FieldValueFactory factory);
    public abstract JsonNodeType nodeType( );
  }

  /**
   * Represents an inferred JSON object. This is the union of all JSON
   * objects seen in the input set. That is, if the first object is
   * <code>{ a: 10 }<code>, and the second is <code>{ "b": "foo" }</code>
   * then the inferred schema is ( a: number, nullable, b: string, nullable ).
   */
  
  // TODO: Detect missing fields & set them nullable
  
  public class JsonObjectNode extends JsonSchemaNode {
    public List<JsonSchemaNode> children = new ArrayList<>( );

    public JsonObjectNode( ) {
      super(DataType.TUPLE);
    }

    @Override
    public JsonSchemaNode doMerge(JsonSchemaNode other, FieldValueFactory factory) {
      if ( ! ( other instanceof JsonObjectNode ) ) {
        throw new ValueConversionError( "Incompatible types: " + type  + " and " +other.type );
      }
      return mergeTuple( (JsonObjectNode) other );
    }

    public JsonObjectNode mergeTuple(JsonObjectNode other) {
      
      // Build a name-to-position index for the other tuple.
      
      Map<String,Integer> otherIndex = new HashMap<>( );
      for ( int i = 0;  i < other.children.size( );  i++ ) {
        otherIndex.put( other.children.get( i ).name, i );
      }
      
      // Scan though the children of this tuple, marking children
      // that have a match with a pointer to the match, and marking
      // the matched other nodes.
      
      int matches[] = new int[ children.size( ) ];
      boolean isMatched[] = new boolean[ other.children.size( ) ];
      for ( int i = 0;  i < children.size( );  i++ ) {
        Integer match = otherIndex.get( children.get( i ).name );
        if ( match == null ) {
          matches[i] = -1;
        } else {
          int matchIndex = match;
          matches[i] = matchIndex;
          isMatched[matchIndex] = true;
        }
      }
      
      // Build the merged schema.
      
      List<JsonSchemaNode> merged = new ArrayList<>( );
      int copyIndex = 0;
      for ( int i = 0;  i < children.size( );  i++ ) {
        JsonSchemaNode child = children.get( i );
        int matchIndex = matches[i];
        
        // If this node has a match, Then insert all unmatched other
        // nodes before the match. Then, merge the matched nodes.
        
        if ( matchIndex != -1 ) {
          while ( copyIndex < matchIndex ) {
            if ( ! isMatched[copyIndex] )
              merged.add( other.children.get( copyIndex ) );
            copyIndex++;
          }
          child = child.merge( other.children.get( matchIndex ), factory );
        }
        merged.add( child );
      }
      
      // Finally, copy any unmatched nodes at the end of the other
      // tuple.
      
      while ( copyIndex < isMatched.length ) {
        if ( ! isMatched[copyIndex] )
          merged.add( other.children.get( copyIndex ) );
        copyIndex++;
      }
      
      // Reuse this tuple, with merged children.
      
      children = merged;
      return this;
    }

    @Override
    public JsonNodeType nodeType() {
      return JsonNodeType.OBJECT;
    }
  }
  
  /**
   * Describes a JSON scalar value (null, boolean, number or string.)
   */
  
  public class JsonScalarNode extends JsonSchemaNode {
    
    public JsonScalarNode( DataType type ) {
      super( type );
    }

    @Override
    public JsonSchemaNode doMerge(JsonSchemaNode other, FieldValueFactory factory) {
      type = factory.mergeTypes( type, ((JsonScalarNode) other).type );
      return this;
    }

    @Override
    public JsonNodeType nodeType() {
      return JsonNodeType.SCALAR;
    }
  }
  
  /**
   * Describes a JSON array, including its inferred member type. The member
   * type can be {@link DataType#UNDEFINED UNDEFINED} if all instances of the
   * sampled array are empty.
   */
  
  public class JsonArrayNode extends JsonSchemaNode {
    public JsonSchemaNode element;
    
    public JsonArrayNode( JsonSchemaNode member ) {
      super(DataType.LIST);
      this.element = member;
    }

    @Override
    public JsonSchemaNode doMerge(JsonSchemaNode other, FieldValueFactory factory) {
      element = element.merge( ((JsonArrayNode) other).element, factory);
      return this;
    }

    @Override
    public JsonNodeType nodeType() {
      return JsonNodeType.ARRAY;
    }
  }
  
  private FieldValueFactory factory;
  private JsonObjectNode jsonSchema;
  
  public ObjectParser( FieldValueFactory factory ) {
    this.factory = factory;
  }
  
  public void addObject( JsonObject obj ) {
    JsonObjectNode thisSchema = parseObject( obj );
    if ( jsonSchema == null )
      jsonSchema = thisSchema;
    else
      jsonSchema = jsonSchema.mergeTuple( thisSchema );
  }
  
  public JsonObjectNode getJsonSchema( ) {
    return jsonSchema;
  }

  public JsonObjectNode parseObject( JsonObject obj ) {
    JsonObjectNode tuple = new JsonObjectNode( );
    for ( String key : obj.keySet() ) {
      JsonValue value = obj.get( key );
      JsonSchemaNode child = parseValue( value );
      child.name = key;
      tuple.children.add( child );
    }
    return tuple;
  }
  
  private JsonSchemaNode parseValue(JsonValue value) {
    DataType type = parseType( value );
    JsonSchemaNode child;
    if ( type == DataType.TUPLE ) {
      child = parseObject( (JsonObject) value );
    } else if ( type == DataType.LIST ) {
      child = buildArray( (JsonArray) value );
    } else {
      child = new JsonScalarNode( type );
    }
    return child;
  }

  public static DataType parseType( JsonValue value ) {
    if ( value == null )
      return DataType.NULL;
    switch ( value.getValueType() ) {
    case TRUE:
    case FALSE:
      return DataType.BOOLEAN;
    case NULL:
      return DataType.NULL;
    case NUMBER:
      JsonNumber number = (JsonNumber) value;
      if ( number.isIntegral() ) {
        return DataType.INT64;
      }
      else {
        return DataType.DECIMAL;
      }
    case STRING:
      return DataType.STRING;
    case OBJECT:
      return DataType.TUPLE;
    case ARRAY:
      return DataType.LIST;
    default:
      throw new ValueConversionError( "Unknown JSON value: " + value.getValueType() );
    }
  }

  private JsonSchemaNode buildArray(JsonArray array) {
    JsonSchemaNode member = null;
    for ( int i = 0;  i < array.size( );  i++ ) {
      JsonValue value = array.get( i );
      JsonSchemaNode thisMember = parseValue( value );
      if ( member == null ) {
        member = thisMember;
      } else {
        member = member.merge( thisMember, factory );
      }
    }
    if ( member == null ) {
      member = new JsonScalarNode( DataType.UNDEFINED );
    }
    return new JsonArrayNode( member );
  }
}
