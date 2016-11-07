package org.apache.drill.jig.api.json;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.extras.json.source.JsonResultCollection;
import org.junit.Test;

public class TestDirectJsonData
{
  @Test
  public void testFlatSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flat.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    validateFlatData( scanner );
  }
  
  @Test
  public void testFlatArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flatArray.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    validateFlatData( scanner );
  }

  private void validateFlatData(JsonResultCollection scanner) throws Exception {
    assertTrue( scanner.next( ) );
    validateTupleSet( scanner.tuples( ) );
    
    assertFalse( scanner.next( ) );
    scanner.close( );
  }
  
  public static void validateTupleSet(TupleSet tuples) throws JigException
  {
    assertTrue( tuples.next() );
    validateTuple1( tuples.tuple() );    
    assertTrue( tuples.next() );
    validateTuple2( tuples.tuple() );
    assertFalse( tuples.next( ) );
  }

  public static void validateTuple1(TupleValue tuple)
  {
    assertNotNull( tuple );
    {
      FieldValue field = tuple.field(0);
      assertTrue( field.type() == DataType.INT64 );
      assertFalse( field.isNull() );
      try {
        field.getArray();
        fail( );
      }
      catch ( ValueConversionError e ) {
        // Expected
      }
      assertNotNull( field );
      assertEquals( 10L, field.getLong() );
      assertEquals( 10, field.getInt() );
      assertEquals( 10, (int) field.getDouble() );
      assertEquals( new BigDecimal( 10 ), field.getDecimal() );
      assertEquals( "10", field.getString( ) );
    }
    
    {
      FieldValue field = tuple.field( "numberField" );
      assertEquals( 10, field.getInt() );
    }
      
    {
      FieldValue field = tuple.field( 1 );
      assertEquals( "foo", field.getString() );
    }
    
    {
      FieldValue field = tuple.field( "stringField" );
      assertEquals( "foo", field.getString() );
    }
    
    {
      FieldValue field = tuple.field( "numberWithNullField" );
      assertTrue( field.isNull() );
    }
    
    {
      FieldValue field = tuple.field( "stringWithNullField" );
      assertTrue( field.isNull() );
    }
    
    {
      FieldValue field = tuple.field( "bool1" );
      assertTrue( field.getBoolean() );
    }
    
    {
      FieldValue field = tuple.field( "bool2" );
      assertFalse( field.getBoolean() );
    }
  }

  public static void validateTuple2(TupleValue tuple)
  {
    {
      FieldValue field = tuple.field( "numberField" );
      assertEquals( 20, field.getInt() );
    }
      
    {
      FieldValue field = tuple.field( "stringField" );
      assertEquals( "bar", field.getString() );
    }
    
    {
      FieldValue field = tuple.field( "numberWithNullField" );
      assertEquals( 120, field.getInt() );
      assertEquals( DataType.INT64, field.type() );
    }
    
    {
      FieldValue field = tuple.field( "stringWithNullField" );
      assertEquals( "mumble", field.getString() );
      assertEquals( DataType.STRING, field.type() );
    }
    
    {
      FieldValue field = tuple.field( "bool1" );
      assertFalse( field.getBoolean() );
    }
    
    {
      FieldValue field = tuple.field( "bool2" );
      assertTrue( field.getBoolean() );
    }
    
  }

  @Test
  public void testArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "array.json" );
    JsonResultCollection scanner = new JsonResultCollection( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.tuples( );
    
    assertTrue( tuples.next() );
    TupleValue tuple = tuples.tuple();
    {
      FieldValue field = tuple.field("index");
      assertEquals( 1, field.getInt() );
    }
    
    {
      FieldValue field = tuple.field("numberArray");
      assertFalse( field.isNull() );
//      assertNull( field.asScalar( ) );
      ArrayValue array = field.getArray();
      assertNotNull( array );
      assertEquals( 3, array.size() );
      assertEquals( 1, array.get( 0 ).getInt() );
      assertEquals( 2, array.get( 1 ).getInt() );
      assertEquals( 3, array.get( 2 ).getInt() );
      try {
        array.get( -1 );
        fail( );
      } catch ( IndexOutOfBoundsException e ) { }
      try {
        array.get( 4 );
        fail( );
      } catch ( IndexOutOfBoundsException e ) { }
    }
    
    {
      FieldValue field = tuple.field("stringArray");
      ArrayValue array = field.getArray();
      assertEquals( 3, array.size() );
      assertEquals( "a", array.get( 0 ).getString() );
      assertEquals( "b", array.get( 1 ).getString() );
      assertEquals( "c", array.get( 2 ).getString() );
    }
    
    {
      FieldValue field = tuple.field("emptyArray");
      ArrayValue array = field.getArray();
      assertEquals( 0, array.size() );
    }
    
    assertFalse( tuples.next( ) );
    assertFalse( scanner.next( ) );
    scanner.close( );
  }
}
