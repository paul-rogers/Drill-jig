package org.apache.drill.jig.extras.json;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.extras.json.ObjectParser.JsonObjectNode;
import org.apache.drill.jig.types.FieldValueFactory;
import org.junit.Test;

public class TestSchemaBuilder {

  @Test
  public void testUnflattened() {
    JsonObject obj = createObject( );
    
    ObjectParser parser = new ObjectParser( new FieldValueFactory( ) );
    JsonObjectNode tuple = parser.parseObject( obj );

    SchemaBuilder3 sb = new SchemaBuilder3( tuple );
    TupleSchema schema = sb.build();
    
    assertEquals( 8, schema.count() );
    
    FieldSchema field = schema.field( 0 );
    assertEquals( "a", field.name() );
    assertTrue( field.nullable() );
    assertEquals( DataType.NULL, field.type() );
    
    field = schema.field( 1 );
    assertEquals( "b", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.BOOLEAN, field.type() );
    
    field = schema.field( 2 );
    assertEquals( "c", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.INT64, field.type() );
    
    field = schema.field( 3 );
    assertEquals( "d", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.STRING, field.type() );
    
    field = schema.field( 4 );
    assertEquals( "e", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.MAP, field.type() );
    
    field = schema.field( 5 );
    assertEquals( "f", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.member().nullable() );
    assertEquals( DataType.UNDEFINED, field.member( ).type() );
    
    field = schema.field( 6 );
    assertEquals( "g", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.member().nullable() );
    assertEquals( DataType.INT64, field.member( ).type() );
    
    field = schema.field( 7 );
    assertEquals( "h", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.member().nullable() );
    assertEquals( DataType.LIST, field.member( ).type() );
    assertFalse( field.member().member().nullable() );
    assertEquals( DataType.STRING, field.member( ).member().type() );
  }

  private JsonObject createObject() {
    Map<String,?> config = new HashMap<>( );
    JsonBuilderFactory factory = Json.createBuilderFactory( config );
    JsonObject obj = factory.createObjectBuilder()
        .add( "a", JsonValue.NULL )
        .add( "b", JsonValue.TRUE )
        .add( "c", 10 )
        .add( "d", "foo" )
        .add( "e", factory.createObjectBuilder()
            .add( "e1", "first" )
            .add( "e2", 12 )
            .build())
        .add( "f", factory.createArrayBuilder().build( ) )
        .add( "g", factory.createArrayBuilder()
            .add( 10 )
            .build( ))
        .add( "h", factory.createArrayBuilder()
            .add( factory.createArrayBuilder()
                .add( "bar" )
                .build( ))
            .build( ))
        .build( );
    return obj;
  }


  @Test
  public void testFlattened() {
    JsonObject obj = createObject( );
    
    ObjectParser parser = new ObjectParser( new FieldValueFactory( ) );
    JsonObjectNode tuple = parser.parseObject( obj );

    SchemaBuilder3 sb = new SchemaBuilder3( tuple );
    sb.flatten( true );
    TupleSchema schema = sb.build();
    
    assertEquals( 9, schema.count() );
    
    assertEquals( "a", schema.field( 0 ).name() );
    assertEquals( "b", schema.field( 1 ).name() );
    assertEquals( "c", schema.field( 2 ).name() );
    assertEquals( "d", schema.field( 3 ).name() );
     
    FieldSchema field = schema.field( 4 );
    assertEquals( "e.e1", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.STRING, field.type() );
    
    field = schema.field( 5 );
    assertEquals( "e.e2", field.name() );
    assertFalse( field.nullable() );
    assertEquals( DataType.INT64, field.type() );
    
    assertEquals( "f", schema.field( 6 ).name() );
    assertEquals( "g", schema.field( 7 ).name() );
    assertEquals( "h", schema.field( 8 ).name() );
  }

}
