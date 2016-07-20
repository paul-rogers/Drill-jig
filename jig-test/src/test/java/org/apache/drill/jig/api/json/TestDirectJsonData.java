package org.apache.drill.jig.api.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor;
import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.TupleAccessor;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.ValueConversionError;
import org.apache.drill.jig.api.json.JsonScanner;
import org.junit.Test;

public class TestDirectJsonData
{
  @Test
  public void testFlatSchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flat.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    validateFlatData( scanner );
  }
  
  @Test
  public void testFlatArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "flatArray.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    validateFlatData( scanner );
  }

  private void validateFlatData(JsonScanner scanner) throws Exception {
    assertTrue( scanner.next( ) );
    validateTupleSet( scanner.getTuples( ) );
    
    assertFalse( scanner.next( ) );
    scanner.close( );
  }
  
  public static void validateTupleSet(TupleSet tuples) throws JigException
  {
    assertTrue( tuples.next() );
    validateTuple1( tuples.getTuple() );    
    assertTrue( tuples.next() );
    validateTuple2( tuples.getTuple() );
    assertFalse( tuples.next( ) );
  }

  public static void validateTuple1(TupleAccessor tuple)
  {
    assertNotNull( tuple );
    {
      FieldAccessor field = tuple.getField(0);
      assertTrue( field.getType() == DataType.INT64 );
      assertTrue( field.getCardinality() == Cardinality.OPTIONAL );
      assertFalse( field.isNull() );
      try {
        field.asArray();
        fail( );
      }
      catch ( ValueConversionError e ) {
        // Expected
      }
      FieldAccessor.ScalarAccessor scalar = field.asScalar();
      assertNotNull( scalar );
      assertEquals( 10L, scalar.getLong() );
      assertEquals( 10, scalar.getInt() );
//      assertEquals( 10, (int) scalar.getDouble() );
//      assertTrue( new BigDecimal( 10 ).equals( scalar.getBigDecimal() ) );
      try {
        scalar.getString();
        fail( );
      }
      catch ( ValueConversionError e ) {
        // Expected
      }
    }
    
    {
      FieldAccessor field = tuple.getField( "numberField" );
      assertEquals( 10, field.asScalar().getInt() );
    }
      
    {
      FieldAccessor field = tuple.getField( 1 );
      assertEquals( "foo", field.asScalar().getString() );
    }
    
    {
      FieldAccessor field = tuple.getField( "stringField" );
      assertEquals( "foo", field.asScalar().getString() );
    }
    
    {
      FieldAccessor field = tuple.getField( "numberWithNullField" );
      assertTrue( field.isNull() );
    }
    
    {
      FieldAccessor field = tuple.getField( "stringWithNullField" );
      assertTrue( field.isNull() );
    }
    
    {
      FieldAccessor field = tuple.getField( "bool1" );
      assertTrue( field.asScalar( ).getBoolean() );
    }
    
    {
      FieldAccessor field = tuple.getField( "bool2" );
      assertFalse( field.asScalar( ).getBoolean() );
    }
  }

  public static void validateTuple2(TupleAccessor tuple)
  {
    {
      FieldAccessor field = tuple.getField( "numberField" );
      assertEquals( 20, field.asScalar().getInt() );
    }
      
    {
      FieldAccessor field = tuple.getField( "stringField" );
      assertEquals( "bar", field.asScalar().getString() );
    }
    
    {
      FieldAccessor field = tuple.getField( "numberWithNullField" );
      assertEquals( 120, field.asAny().getInt() );
      assertEquals( DataType.INT64, field.asAny().getDataType() );
    }
    
    {
      FieldAccessor field = tuple.getField( "stringWithNullField" );
      assertEquals( "mumble", field.asAny().getString() );
      assertEquals( DataType.STRING, field.asAny().getDataType() );
    }
    
    {
      FieldAccessor field = tuple.getField( "bool1" );
      assertFalse( field.asScalar( ).getBoolean() );
    }
    
    {
      FieldAccessor field = tuple.getField( "bool2" );
      assertTrue( field.asScalar( ).getBoolean() );
    }
    
  }

  @Test
  public void testArraySchema() throws Exception {
    InputStream in = getClass( ).getResourceAsStream( "array.json" );
    JsonScanner scanner = new JsonScanner( new InputStreamReader( in, "UTF-8" ) );
    assertTrue( scanner.next( ) );
    TupleSet tuples = scanner.getTuples( );
    
    assertTrue( tuples.next() );
    TupleAccessor tuple = tuples.getTuple();
    {
      FieldAccessor field = tuple.getField("index");
      assertEquals( 1, field.asScalar( ).getInt() );
    }
    
    {
      FieldAccessor field = tuple.getField("numberArray");
      assertFalse( field.isNull() );
//      assertNull( field.asScalar( ) );
      FieldAccessor.ArrayAccessor array = field.asArray();
      assertNotNull( array );
      assertEquals( 3, array.size() );
      assertEquals( 1, array.get( 0 ).asScalar().getInt() );
      assertEquals( 2, array.get( 1 ).asScalar().getInt() );
      assertEquals( 3, array.get( 2 ).asScalar().getInt() );
      assertNull( array.get( -1 ) );
      assertNull( array.get( 4 ) );
    }
    
    {
      FieldAccessor field = tuple.getField("stringArray");
      FieldAccessor.ArrayAccessor array = field.asArray();
      assertEquals( 3, array.size() );
      assertEquals( "a", array.get( 0 ).asScalar().getString() );
      assertEquals( "b", array.get( 1 ).asScalar().getString() );
      assertEquals( "c", array.get( 2 ).asScalar().getString() );
      assertNull( array.get( -1 ) );
      assertNull( array.get( 4 ) );
    }
    
    {
      FieldAccessor field = tuple.getField("emptyArray");
      FieldAccessor.ArrayAccessor array = field.asArray();
      assertEquals( 0, array.size() );
    }
    
    assertFalse( tuples.next( ) );
    assertFalse( scanner.next( ) );
    scanner.close( );
  }
}
