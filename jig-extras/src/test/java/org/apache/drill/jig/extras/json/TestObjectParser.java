package org.apache.drill.jig.extras.json;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.extras.json.source.ObjectParser;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonArrayNode;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonObjectNode;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonScalarNode;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonSchemaNode;
import org.apache.drill.jig.types.FieldValueFactory;
import org.junit.Test;

public class TestObjectParser {

  /**
   * Test the object parser on a single node. Because
   * parsing is recursive, this also tests merging of fields
   * (types, nulls).
   */
  
  @Test
  public void testSingle( ) {
    Map<String,?> config = new HashMap<>( );
    JsonBuilderFactory factory = Json.createBuilderFactory( config );
    JsonObject obj = factory.createObjectBuilder()
        .add( "a", "a-value" )
        .add( "b", 20 )
        .add( "c", 25.5 )
        .add( "d", factory.createObjectBuilder()
          .add( "da", "c-value" )
          .add( "db", 40 )
          .build( ) )
        .add( "e", JsonValue.NULL )
        .add( "f", JsonValue.TRUE )
        .add( "g", JsonValue.FALSE )
        .add( "h", factory.createArrayBuilder()
            .add( "a-1" )
            .add( "a-2" )
            .build( ))
        .add( "i", factory.createArrayBuilder().build( ))
        .add( "j", factory.createArrayBuilder()
            .add( factory.createArrayBuilder()
                .add( "j1-1" )
                .add( "j1-2" )
                .build( ))
            .add( factory.createArrayBuilder()
                .add( "jd-1" )
                .add( "jd-2" )
                .build( ))
            .build( ))
        .add( "k", factory.createArrayBuilder( )
            .add( "str" )
            .add( 50 )
            .build( ))
        .add( "l", factory.createArrayBuilder()
            .add( "a-1" )
            .add( JsonValue.NULL )
            .build( ))
        .add( "m", factory.createArrayBuilder()
            .add( factory.createArrayBuilder()
                .add( "j1-1" )
                .build( ))
            .add( factory.createArrayBuilder()
                .add( JsonValue.NULL )
                .build( ))
            .build( ))
        .add( "n", factory.createArrayBuilder()
            .add( factory.createArrayBuilder()
                .add( "n1-1" )
                 .build( ))
            .add( factory.createArrayBuilder()
                .add( JsonValue.TRUE )
                .build( ))
            .build( ))
        .add( "o", factory.createArrayBuilder()
            .add( factory.createArrayBuilder()
                .add( "n1-1" )
                 .build( ))
            .add( JsonValue.NULL )
            .build( ))
        .build( );
    
    ObjectParser parser = new ObjectParser( new FieldValueFactory( ) );
    JsonObjectNode tuple = parser.parseObject( obj );
    assertEquals( 15, tuple.children.size() );
    
    // String
    
    JsonSchemaNode child = tuple.children.get( 0 );
    assertEquals( "a", child.name );
    assertEquals( DataType.STRING, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonScalarNode );
    
    // Number (int)
    
    child = tuple.children.get( 1 );
    assertEquals( "b", child.name );
    assertEquals( DataType.NUMBER, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonScalarNode );
    
    // Number (float)
    
    child = tuple.children.get( 2 );
    assertEquals( "c", child.name );
    assertEquals( DataType.NUMBER, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonScalarNode );
    
    // Tuple
    
    child = tuple.children.get( 3 );
    assertEquals( "d", child.name );
    assertEquals( DataType.TUPLE, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonObjectNode );
    
    JsonObjectNode childTuple = (JsonObjectNode) child;
    assertEquals( 2, childTuple.children.size( ) );
    
    JsonSchemaNode grandChild = childTuple.children.get( 0 );
    assertEquals( "da", grandChild.name );
    assertEquals( DataType.STRING, grandChild.type );
    assertFalse( grandChild.nullable );
    assertTrue( grandChild instanceof JsonScalarNode );
    
    grandChild = childTuple.children.get( 1 );
    assertEquals( "db", grandChild.name );
    assertEquals( DataType.NUMBER, grandChild.type );
    assertFalse( grandChild.nullable );
    assertTrue( grandChild instanceof JsonScalarNode );
    
    // Null
    
    child = tuple.children.get( 4 );
    assertEquals( "e", child.name );
    assertEquals( DataType.NULL, child.type );
    assertTrue( child.nullable );
    assertTrue( child instanceof JsonScalarNode );
    
    // Boolean (true)
    
    child = tuple.children.get( 5 );
    assertEquals( "f", child.name );
    assertEquals( DataType.BOOLEAN, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonScalarNode );
    
    // Boolean (false)
    
    child = tuple.children.get( 6 );
    assertEquals( "g", child.name );
    assertEquals( DataType.BOOLEAN, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonScalarNode );
    
    // List (single type)
    
    child = tuple.children.get( 7 );
    assertEquals( "h", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    JsonSchemaNode member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( member.nullable );
    assertTrue( member instanceof JsonScalarNode );  
    
    // List (empty)
    
    child = tuple.children.get( 8 );
    assertEquals( "i", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.UNDEFINED, member.type );
    assertFalse( member.nullable );
    assertTrue( member instanceof JsonScalarNode );

    // List of List of single type
    
    child = tuple.children.get( 9 );
    assertEquals( "j", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.LIST, member.type );
    assertFalse( member.nullable );
    assertTrue( member instanceof JsonArrayNode );
    
    JsonSchemaNode member2 = ((JsonArrayNode) member).element;
    assertNull( member2.name );
    assertEquals( DataType.STRING, member2.type );
    assertFalse( member2.nullable );
    assertTrue( member2 instanceof JsonScalarNode );
    
    // Variant list
    
    child = tuple.children.get( 10 );
    assertEquals( "k", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.VARIANT, member.type );
    assertFalse( member.nullable );
    assertTrue( member instanceof JsonScalarNode );
    
    // List with Nulls
    
    child = tuple.children.get( 11 );
    assertEquals( "l", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.STRING, member.type );
    assertTrue( member.nullable );
    assertTrue( member instanceof JsonScalarNode );
    
    // Nested lists, leaf nulls
    
    child = tuple.children.get( 12 );
    assertEquals( "m", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.LIST, member.type );
    assertFalse( member.nullable );
    assertTrue( member instanceof JsonArrayNode );
    
    member2 = ((JsonArrayNode) member).element;
    assertNull( member2.name );
    assertEquals( DataType.STRING, member2.type );
    assertTrue( member2.nullable );
    assertTrue( member2 instanceof JsonScalarNode );
    
    // Nested lists, leaf variant
    
    child = tuple.children.get( 13 );
    assertEquals( "n", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.LIST, member.type );
    assertFalse( member.nullable );
    assertTrue( member instanceof JsonArrayNode );
    
    member2 = ((JsonArrayNode) member).element;
    assertNull( member2.name );
    assertEquals( DataType.VARIANT, member2.type );
    assertFalse( member2.nullable );
    assertTrue( member2 instanceof JsonScalarNode );
    
    // Nested lists, with null lists
    
    child = tuple.children.get( 14 );
    assertEquals( "o", child.name );
    assertEquals( DataType.LIST, child.type );
    assertFalse( child.nullable );
    assertTrue( child instanceof JsonArrayNode );
    
    member = ((JsonArrayNode) child).element;
    assertNull( member.name );
    assertEquals( DataType.LIST, member.type );
    assertTrue( member.nullable );
    assertTrue( member instanceof JsonArrayNode );
    
    member2 = ((JsonArrayNode) member).element;
    assertNull( member2.name );
    assertEquals( DataType.STRING, member2.type );
    assertFalse( member2.nullable );
    assertTrue( member2 instanceof JsonScalarNode );   
  }
  
  @Test
  public void testMergeTuple( ) {
    Map<String,?> config = new HashMap<>( );
    JsonBuilderFactory factory = Json.createBuilderFactory( config );
    JsonObject obj1 = factory.createObjectBuilder()
        .add( "a", "a-value" )
        .add( "b", "b-value" )
        .add( "c", "c-value" )
        .add( "d", "d-value" )
        .build( );
    JsonObject obj2 = factory.createObjectBuilder()
        .add( "w", "w-value" )
        .add( "a", "a-value" )
        .add( "x", "x-value" )
        .add( "d", "d-value" )
        .add( "y", "y-value" )
        .add( "c", "c-value" )
        .add( "z", "z-value" )
        .build( );
    
    ObjectParser parser = new ObjectParser( new FieldValueFactory( ) );
    JsonObjectNode tuple1 = parser.parseObject( obj1 );
    JsonObjectNode tuple2 = parser.parseObject( obj2 );
    JsonObjectNode tuple3 = tuple1.mergeTuple( tuple2 );
    
    assertEquals( 8, tuple3.children.size() );
    assertEquals( "w", tuple3.children.get( 0 ).name );
    assertEquals( "a", tuple3.children.get( 1 ).name );
    assertEquals( "b", tuple3.children.get( 2 ).name );
    assertEquals( "x", tuple3.children.get( 3 ).name );
    assertEquals( "y", tuple3.children.get( 4 ).name );
    assertEquals( "c", tuple3.children.get( 5 ).name );
    assertEquals( "d", tuple3.children.get( 6 ).name );
    assertEquals( "z", tuple3.children.get( 7 ).name );
  }
}
