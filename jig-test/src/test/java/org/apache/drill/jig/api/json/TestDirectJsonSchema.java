package org.apache.drill.jig.api.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.extras.json.JsonScanner;
import org.junit.Test;

public class TestDirectJsonSchema
{
  @Test
  public void testEmptySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "empty.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    assertFalse( scanner.next( ) );
    scanner.close( );
  }

  @Test
  public void testFlatSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flat.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    validateFlatSchema( scanner );
  }
  
  @Test
  public void testFlatArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flatArray.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    validateFlatSchema( scanner );
  }
  
  public void validateFlatSchema( JsonScanner scanner ) throws Exception
  {
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.getTuples( );
    TupleSchema schema = tuples.schema( );
    assertEquals( 6, schema.count() );
    
    {
      FieldSchema field = schema.field( 0 );
      assertEquals( "numberField", field.name() );
      assertEquals( 0, field.index() );
      assertNotNull( schema.field( "numberField" ) );
      assertEquals( 0, schema.field( "numberField" ).index( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 1 );
      assertEquals( "stringField", field.name() );
      assertEquals( 1, field.index() );
      assertNotNull( schema.field( "stringField" ) );
      assertEquals( 1, schema.field( "stringField" ).index( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.STRING, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 2 );
      assertEquals( "numberWithNullField", field.name() );
      assertEquals( 2, field.index() );
      assertNotNull( schema.field( "numberWithNullField" ) );
      assertEquals( 2, schema.field( "numberWithNullField" ).index( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.VARIANT, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 3 );
      assertEquals( "stringWithNullField", field.name() );
      assertEquals( 3, field.index() );
      assertNotNull( schema.field( "stringWithNullField" ) );
      assertEquals( 3, schema.field( "stringWithNullField" ).index( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.VARIANT, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 4 );
      assertEquals( "bool1", field.name() );
      assertEquals( 4, field.index() );
      assertNotNull( schema.field( "bool1" ) );
      assertEquals( 4, schema.field( "bool1" ).index( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.BOOLEAN, field.type() );
    }
    
    {
      FieldSchema field = schema.field( 5 );
      assertEquals( "bool2", field.name() );
      assertEquals( 5, field.index() );
      assertNotNull( schema.field( "bool2" ) );
      assertEquals( 5, schema.field( "bool2" ).index( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.BOOLEAN, field.type() );
    }
    scanner.close( );
  }

  @Test
  public void testArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "array.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.getTuples( );
    TupleSchema schema = tuples.schema( );
    assertEquals( 4, schema.count() );
    
    {
      FieldSchema field = schema.field( "index" );
      assertEquals( 0, field.index() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( "numberArray" );
      assertEquals( 1, field.index() );
      assertEquals( Cardinality.REPEATED, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( "stringArray" );
      assertEquals( 2, field.index() );
      assertEquals( Cardinality.REPEATED, field.getCardinality() );
      assertEquals( DataType.STRING, field.type() );
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
      assertEquals( Cardinality.REPEATED, field.getCardinality() );
      assertEquals( DataType.VARIANT, field.type() );
    }
    scanner.close( );
  }

  @Test
  public void testMapSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "map.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.getTuples( );
    TupleSchema schema = tuples.schema( );
    assertEquals( 3, schema.count() );
    
    {
      FieldSchema field = schema.field( "index" );
      assertEquals( 0, field.index() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
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
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldSchema field = schema.field( "map.b" );
      assertEquals( 2, field.index() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.STRING, field.type() );
    }
    
    // Not supported by Drill
    
//    {
//      FieldSchema field = schema.getField( "numberMap" );
//      assertEquals( 1, field.getIndex() );
//      assertEquals( FieldCardinality.Optional, field.getCardinality() );
//      assertEquals( FieldType.MAP, field.getType() );
//    }
    
    // Not supported by Drill
    
//    {
//      FieldSchema field = schema.getField( "complexMap" );
//      assertEquals( 1, field.getIndex() );
//      assertEquals( FieldCardinality.Optional, field.getCardinality() );
//      assertEquals( FieldType.COMPLEX_MAP, field.getType() );
//    }
    scanner.close( );
  }
    
}
