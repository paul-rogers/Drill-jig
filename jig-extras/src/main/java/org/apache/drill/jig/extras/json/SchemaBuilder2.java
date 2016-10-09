package org.apache.drill.jig.extras.json;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.extras.json.JsonAccessor.*;
import org.apache.drill.jig.extras.json.ObjectParser.JsonArrayNode;
import org.apache.drill.jig.extras.json.ObjectParser.JsonScalarNode;
import org.apache.drill.jig.extras.json.ObjectParser.JsonSchemaNode;
import org.apache.drill.jig.extras.json.ObjectParser.JsonObjectNode;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;

public class SchemaBuilder2 {

  private TupleObjectAccessor rootAccessor;
  private FieldValue fieldValues[];
  private JsonObjectNode tuple;

  public SchemaBuilder2( JsonObjectNode tuple ) {
    this.tuple = tuple;
  }
  
  public void build( ) {
    TupleObjectAccessor rootAccessor = new TupleObjectAccessor( );
    fieldValues = new FieldValue[ tuple.children.size() ];
    for ( int i = 0;  i < tuple.children.size( );  i++ ) {
      JsonSchemaNode node = tuple.children.get( i );
      ObjectAccessor memberAccessor = new JsonObjectMemberAccessor( rootAccessor, node.memberName );
      ObjectAccessor fieldAccessor;
      switch ( node.nodeType( ) ) {
      case ARRAY:
        buildArray( (JsonArrayNode) node, memberAccessor );
        break;
      case OBJECT:
        buildMap( (JsonObjectNode) node, memberAccessor );
        break;
      case SCALAR:
        fieldValues[i] = buildValueField( (JsonScalarNode) node, memberAccessor );
        break;
      default:
        break;
      
      }
    }
    
  }

  private void buildValueField(JsonScalarNode node, ObjectAccessor baseAccessor) {
    ObjectAccessor accessor;
    if ( node.type.isUndefined() ) {
      accessor = new NullAccessor( );
      return new ScalarDef( node.type, node.nullable, accessor );
    } else if ( node.type.isVariant() ) {
      accessor = new JsonValueAccessor( baseAccessor );
      return new ScalarDef( node.type, node.nullable, accessor );
    } else {
      accessor = new JsonValueAccessor( baseAccessor );
      return new ScalarDef( node.type, node.nullable, accessor );
    }
    
  }

  private void buildArray(JsonArrayNode node, ObjectAccessor memberAccessor) {
    // TODO Auto-generated method stub
    
  }

  private void buildMap(JsonObjectNode node, ObjectAccessor memberAccessor) {
    // TODO Auto-generated method stub
    
  }
}
