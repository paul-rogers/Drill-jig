package org.apache.drill.jig.api.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.ScalarValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.extras.json.JsonScanner;
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

  public static void validateTuple1(TupleValue tuple)
  {
    assertNotNull( tuple );
    {
      FieldValue field = tuple.field(0);
      assertTrue( field.type() == DataType.INT64 );
      assertTrue( field.getCardinality() == Cardinality.OPTIONAL );
      assertFalse( field.isNull() );
      try {
        field.asArray();
        fail( );
      }
      catch ( ValueConversionError e ) {
        // Expected
      }
      ScalarValue scalar = field.asScalar();
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
      FieldValue field = tuple.field( "numberField" );
      assertEquals( 10, field.asScalar().getInt() );
    }
      
    {
      FieldValue field = tuple.field( 1 );
      assertEquals( "foo", field.asScalar().getString() );
    }
    
    {
      FieldValue field = tuple.field( "stringField" );
      assertEquals( "foo", field.asScalar().getString() );
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
      assertTrue( field.asScalar( ).getBoolean() );
    }
    
    {
      FieldValue field = tuple.field( "bool2" );
      assertFalse( field.asScalar( ).getBoolean() );
    }
  }

  public static void validateTuple2(TupleValue tuple)
  {
    {
      FieldValue field = tuple.field( "numberField" );
      assertEquals( 20, field.asScalar().getInt() );
    }
      
    {
      FieldValue field = tuple.field( "stringField" );
      assertEquals( "bar", field.asScalar().getString() );
    }
    
    {
      FieldValue field = tuple.field( "numberWithNullField" );
      assertEquals( 120, field.asAny().getInt() );
      assertEquals( DataType.INT64, field.asAny().getDataType() );
    }
    
    {
      FieldValue field = tuple.field( "stringWithNullField" );
      assertEquals( "mumble", field.asAny().getString() );
      assertEquals( DataType.STRING, field.asAny().getDataType() );
    }
    
    {
      FieldValue field = tuple.field( "bool1" );
      assertFalse( field.asScalar( ).getBoolean() );
    }
    
    {
      FieldValue field = tuple.field( "bool2" );
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
    TupleValue tuple = tuples.getTuple();
    {
      FieldValue field = tuple.field("index");
      assertEquals( 1, field.asScalar( ).getInt() );
    }
    
    {
      FieldValue field = tuple.field("numberArray");
      assertFalse( field.isNull() );
//      assertNull( field.asScalar( ) );
      ArrayValue array = field.asArray();
      assertNotNull( array );
      assertEquals( 3, array.size() );
      assertEquals( 1, array.get( 0 ).asScalar().getInt() );
      assertEquals( 2, array.get( 1 ).asScalar().getInt() );
      assertEquals( 3, array.get( 2 ).asScalar().getInt() );
      assertNull( array.get( -1 ) );
      assertNull( array.get( 4 ) );
    }
    
    {
      FieldValue field = tuple.field("stringArray");
      ArrayValue array = field.asArray();
      assertEquals( 3, array.size() );
      assertEquals( "a", array.get( 0 ).asScalar().getString() );
      assertEquals( "b", array.get( 1 ).asScalar().getString() );
      assertEquals( "c", array.get( 2 ).asScalar().getString() );
      assertNull( array.get( -1 ) );
      assertNull( array.get( 4 ) );
    }
    
    {
      FieldValue field = tuple.field("emptyArray");
      ArrayValue array = field.asArray();
      assertEquals( 0, array.size() );
    }
    
    assertFalse( tuples.next( ) );
    assertFalse( scanner.next( ) );
    scanner.close( );
  }
}
