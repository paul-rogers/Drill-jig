package org.apache.drill.jig.api.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.extras.json.source.JsonResultCollection;
import org.junit.Test;

public class TestDirectJsonSchema
{
  @Test
  public void testEmptySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "empty.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    assertFalse( scanner.next( ) );
    scanner.close( );
  }

  @Test
  public void testFlatSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flat.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    validateFlatSchema( scanner );
  }
  
  @Test
  public void testFlatArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flatArray.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    validateFlatSchema( scanner );
  }
  
  public void validateFlatSchema( JsonResultCollection scanner ) throws Exception
  {
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.tuples( );
    TupleSchema schema = tuples.schema( );
    assertEquals( 6, schema.count() );
    
    {
      FieldSchema field = schema.field( 0 );
      assertEquals( "numberField", field.name() );
      assertEquals( 0, field.index() );
      assertNotNull( schema.field( "numberField" ) );
      assertEquals( 0, schema.field( "numberField" ).index( ) );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 1 );
      assertEquals( "stringField", field.name() );
      assertEquals( 1, field.index() );
      assertNotNull( schema.field( "stringField" ) );
      assertEquals( 1, schema.field( "stringField" ).index( ) );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.STRING, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 2 );
      assertEquals( "numberWithNullField", field.name() );
      assertEquals( 2, field.index() );
      assertNotNull( schema.field( "numberWithNullField" ) );
      assertEquals( 2, schema.field( "numberWithNullField" ).index( ) );
      assertTrue( field.nullable( ) );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 3 );
      assertEquals( "stringWithNullField", field.name() );
      assertEquals( 3, field.index() );
      assertNotNull( schema.field( "stringWithNullField" ) );
      assertEquals( 3, schema.field( "stringWithNullField" ).index( ) );
      assertTrue( field.nullable( ) );
      assertEquals( DataType.STRING, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 4 );
      assertEquals( "bool1", field.name() );
      assertEquals( 4, field.index() );
      assertNotNull( schema.field( "bool1" ) );
      assertEquals( 4, schema.field( "bool1" ).index( ) );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.BOOLEAN, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 5 );
      assertEquals( "bool2", field.name() );
      assertEquals( 5, field.index() );
      assertNotNull( schema.field( "bool2" ) );
      assertEquals( 5, schema.field( "bool2" ).index( ) );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.BOOLEAN, field.type() );
    }
    scanner.close( );
  }

  @Test
  public void testArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "array.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.tuples( );
    TupleSchema schema = tuples.schema( );
    assertEquals( 4, schema.count() );
    
    {
      FieldSchema field = schema.field( "index" );
      assertEquals( 0, field.index() );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( "numberArray" );
      assertEquals( 1, field.index() );
      assertEquals( DataType.LIST, field.type() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.INT64, field.member( ).type() );
    }
    
    {
      FieldSchema field = schema.field( "stringArray" );
      assertEquals( 2, field.index() );
      assertEquals( DataType.LIST, field.type() );
      assertEquals( DataType.STRING, field.member( ).type() );
    }
    
    // Not supported by Drill
    
//    {
//      FieldSchema field = schema.getField( "numberWithNullArray" );
//      assertEquals( 3, field.getIndex() );
//      assertEquals( FieldCardinality.Repeated, field.getCardinality() );
//      assertEquals( FieldType.DOUBLE, field.getType() );
//    }   
    // Not supported by Drill
    
//    {
//      FieldSchema field = schema.getField( "mixedArray" );
//      assertEquals( 3, field.getIndex() );
//      assertEquals( FieldCardinality.Repeated, field.getCardinality() );
//      assertEquals( FieldType.ANY, field.getType() );
//    }
    // Not supported by Drill
    
//    {
//      FieldSchema field = schema.getField( "mixedArray" );
//      assertEquals( 4, field.getIndex() );
//      assertEquals( FieldCardinality.Repeated, field.getCardinality() );
//      assertEquals( FieldType.ANY, field.getType() );
//    }
//    
    // Not supported by Drill
    
//    {
//      FieldSchema field = schema.getField( "nullArray" );
//      assertEquals( 5, field.getIndex() );
//      assertEquals( FieldCardinality.Repeated, field.getCardinality() );
//      assertEquals( FieldType.ANY, field.getType() );
//    }
    
    {
      FieldSchema field = schema.field( "emptyArray" );
      assertEquals( 3, field.index() );
      assertEquals( DataType.LIST, field.type() );
      assertEquals( DataType.UNDEFINED, field.member( ).type() );
    }
    scanner.close( );
  }

  @Test
  public void testMapSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "map.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ), true );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.tuples( );
    TupleSchema schema = tuples.schema( );
    assertEquals( 3, schema.count() );
    
    {
      FieldSchema field = schema.field( "index" );
      assertEquals( 0, field.index() );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.INT64, field.type() );
    }   
    
    // Empty map is ignored in flatten process.
    
//    {
//      FieldSchema field = schema.getField( "emptyMap" );
//      assertEquals( 1, field.getIndex() );
//      assertEquals( FieldCardinality.Optional, field.getCardinality() );
//      assertEquals( FieldType.MAP, field.getType() );
//    }
    
    // Normal map is expanded to fields in flatten process.
    
    {
      FieldSchema field = schema.field( "map.a" );
      assertEquals( 1, field.index() );
      assertEquals( "map.a", field.name() );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( "map.b" );
      assertEquals( 2, field.index() );
      assertFalse( field.nullable( ) );
      assertEquals( DataType.STRING, field.type() );
    }
    scanner.close( );
  }    
}
