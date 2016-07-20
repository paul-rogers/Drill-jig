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
import org.apache.drill.jig.api.json.JsonScanner;
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
    TupleSchema schema = tuples.getSchema( );
    assertEquals( 6, schema.getCount() );
    
    {
      FieldSchema field = schema.getField( 0 );
      assertEquals( "numberField", field.getName() );
      assertEquals( 0, field.getIndex() );
      assertNotNull( schema.getField( "numberField" ) );
      assertEquals( 0, schema.getField( "numberField" ).getIndex( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.INT64, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( 1 );
      assertEquals( "stringField", field.getName() );
      assertEquals( 1, field.getIndex() );
      assertNotNull( schema.getField( "stringField" ) );
      assertEquals( 1, schema.getField( "stringField" ).getIndex( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.STRING, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( 2 );
      assertEquals( "numberWithNullField", field.getName() );
      assertEquals( 2, field.getIndex() );
      assertNotNull( schema.getField( "numberWithNullField" ) );
      assertEquals( 2, schema.getField( "numberWithNullField" ).getIndex( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.ANY, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( 3 );
      assertEquals( "stringWithNullField", field.getName() );
      assertEquals( 3, field.getIndex() );
      assertNotNull( schema.getField( "stringWithNullField" ) );
      assertEquals( 3, schema.getField( "stringWithNullField" ).getIndex( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.ANY, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( 4 );
      assertEquals( "bool1", field.getName() );
      assertEquals( 4, field.getIndex() );
      assertNotNull( schema.getField( "bool1" ) );
      assertEquals( 4, schema.getField( "bool1" ).getIndex( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.BOOLEAN, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( 5 );
      assertEquals( "bool2", field.getName() );
      assertEquals( 5, field.getIndex() );
      assertNotNull( schema.getField( "bool2" ) );
      assertEquals( 5, schema.getField( "bool2" ).getIndex( ) );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.BOOLEAN, field.getType() );
    }
    scanner.close( );
  }

  @Test
  public void testArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "array.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.getTuples( );
    TupleSchema schema = tuples.getSchema( );
    assertEquals( 4, schema.getCount() );
    
    {
      FieldSchema field = schema.getField( "index" );
      assertEquals( 0, field.getIndex() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.INT64, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( "numberArray" );
      assertEquals( 1, field.getIndex() );
      assertEquals( Cardinality.REPEATED, field.getCardinality() );
//      assertNull( field.getStructure() );
      assertEquals( DataType.INT64, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( "stringArray" );
      assertEquals( 2, field.getIndex() );
      assertEquals( Cardinality.REPEATED, field.getCardinality() );
      assertEquals( DataType.STRING, field.getType() );
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
      FieldSchema field = schema.getField( "emptyArray" );
      assertEquals( 3, field.getIndex() );
      assertEquals( Cardinality.REPEATED, field.getCardinality() );
      assertEquals( DataType.ANY, field.getType() );
    }
    scanner.close( );
  }

  @Test
  public void testMapSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "map.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.getTuples( );
    TupleSchema schema = tuples.getSchema( );
    assertEquals( 3, schema.getCount() );
    
    {
      FieldSchema field = schema.getField( "index" );
      assertEquals( 0, field.getIndex() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.INT64, field.getType() );
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
      FieldSchema field = schema.getField( "map.a" );
      assertEquals( 1, field.getIndex() );
      assertEquals( "map.a", field.getName() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.INT64, field.getType() );
    }
    
    {
      FieldSchema field = schema.getField( "map.b" );
      assertEquals( 2, field.getIndex() );
      assertEquals( Cardinality.OPTIONAL, field.getCardinality() );
      assertEquals( DataType.STRING, field.getType() );
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
