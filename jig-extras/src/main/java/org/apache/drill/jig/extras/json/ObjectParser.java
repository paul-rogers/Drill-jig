package org.apache.drill.jig.extras.json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.FieldValueFactory;

public class ObjectParser {
  
  public abstract class SchemaNode {
    public DataType type;   
    public String name;
    public boolean nullable = false;
    
    public SchemaNode( DataType type ) {
      this.type = type;
      nullable = type == DataType.NULL;
    }
    
    public SchemaNode merge(SchemaNode other, FieldValueFactory factory) {
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
      SchemaNode result = doMerge( other, factory );
      result.nullable |= other.nullable;
      return result;                       
    }

    protected abstract SchemaNode doMerge(SchemaNode other, FieldValueFactory factory);
  }

  public class TupleNode extends SchemaNode {
    List<SchemaNode> children = new ArrayList<>( );

    public TupleNode( ) {
      super(DataType.TUPLE);
    }

    @Override
    public SchemaNode doMerge(SchemaNode other, FieldValueFactory factory) {
      if ( ! ( other instanceof FieldNode ) ) {
        throw new ValueConversionError( "Incompatible types: " + type  + " and " +other.type );
      }
      return mergeTuple( (TupleNode) other );
    }

    public TupleNode mergeTuple(TupleNode other) {
      
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
      
      List<SchemaNode> merged = new ArrayList<>( );
      int copyIndex = 0;
      for ( int i = 0;  i < children.size( );  i++ ) {
        SchemaNode child = children.get( i );
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
  }
  
  public class FieldNode extends SchemaNode {
    
    public FieldNode( DataType type ) {
      super( type );
    }

    @Override
    public SchemaNode doMerge(SchemaNode other, FieldValueFactory factory) {
      type = factory.mergeTypes( type, ((FieldNode) other).type );
      return this;
    }
  }
  
  public class ArrayNode extends SchemaNode {
    SchemaNode member;
    
    public ArrayNode( SchemaNode member ) {
      super(DataType.LIST);
      this.member = member;
    }

    @Override
    public SchemaNode doMerge(SchemaNode other, FieldValueFactory factory) {
      member = member.merge( ((ArrayNode) other).member, factory);
      return this;
    }
  }
  
  private FieldValueFactory factory;
  
  public ObjectParser( FieldValueFactory factory ) {
    this.factory = factory;
  }

  public TupleNode parseObject( JsonObject obj ) {
    TupleNode tuple = new TupleNode( );
    for ( String key : obj.keySet() ) {
      JsonValue value = obj.get( key );
      SchemaNode child = parseValue( value );
      child.name = key;
      tuple.children.add( child );
    }
    return tuple;
  }
  
  private SchemaNode parseValue(JsonValue value) {
    DataType type = parseType( value );
    SchemaNode child;
    if ( type == DataType.TUPLE ) {
      child = parseObject( (JsonObject) value );
    } else if ( type == DataType.LIST ) {
      child = buildArray( (JsonArray) value );
    } else {
      child = new FieldNode( type );
    }
    return child;
  }

  public static DataType parseType( JsonValue value ) {
    switch ( value.getValueType() ) {
    case TRUE:
    case FALSE:
      return DataType.BOOLEAN;
    case NULL:
      return DataType.VARIANT;
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

  private SchemaNode buildArray(JsonArray array) {
    SchemaNode member = null;
    for ( int i = 0;  i < array.size( );  i++ ) {
      JsonValue value = array.get( i );
      SchemaNode thisMember = parseValue( value );
      if ( member == null ) {
        member = thisMember;
      } else {
        member = member.merge( thisMember, factory );
      }
    }
    if ( member == null ) {
      member = new FieldNode( DataType.UNDEFINED );
    }
    return new ArrayNode( member );
  }
}
