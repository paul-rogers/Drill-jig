package org.apache.drill.jig.api.array;

import static org.junit.Assert.*;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.ArrayResultCollection;
import org.apache.drill.jig.extras.array.Batch;
import org.junit.Test;

public class TestArrays {
  
  /**
   * Test obtaining the type from the array declaration, for type String.
   * 
   * @throws JigException
   */
  
  @Test
  public void testStringArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[] { "first", "second" } },
          { new String[] { "a", "b", "c" } }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    assertEquals( DataType.STRING, array.get(0).type() );
    assertEquals( Array.get( batch.data[0][0], 0), array.get(0).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    Helpers.compareArrays( batch.data[1][0], tuple.field( 0 ).getArray() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Test obtaining the type from the array declaration, for type Decimal.
   * 
   * @throws JigException
   */
  
  @Test
  public void testDecimalArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new BigDecimal[] { new BigDecimal( 10 ), new BigDecimal( 20 ) } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.DECIMAL, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Typed object array, with null members.
   * 
   * @throws JigException
   */
  
  @Test
  public void testTypedNullableArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[] { "first", "second", null } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertTrue( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Typed object array, with null list fields.
   * 
   * @throws JigException
   */
  
  @Test
  public void testNullableTypedNullableArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[] { "first", "second", null } },
          { null }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertTrue( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertTrue( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Generic object array, with the type inferred from the members.
   * All of the same type.
   * 
   * @throws JigException
   */
  
  @Test
  public void testTypedObjectArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[] { "first", "second" } },
          { new Object[] { "a", "b", "c" } }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    assertEquals( DataType.STRING, array.get(0).type() );
    assertEquals( Array.get( batch.data[0][0], 0), array.get(0).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    Helpers.compareArrays( batch.data[1][0], tuple.field( 0 ).getArray() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Empty array, but the array type provides type info.
   * 
   * @throws JigException
   */
  
  @Test
  public void testEmptyTypedArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[] { } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Object array, no type info available.
   * 
   * @throws JigException
   */

  @Test
  public void testEmptyObjectArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[] { } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.UNDEFINED, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
        
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testVariantArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[] { "first", (int) 10, (float) 12.5 } },
          { null }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertTrue( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.VARIANT, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( DataType.STRING, array.get( 0 ).type() );
    assertEquals( DataType.INT32, array.get( 1 ).type() );
    assertEquals( DataType.FLOAT32, array.get( 2 ).type() );
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  
  @Test
  public void testVariantNullableArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new Object[] { "first", (int) 10, (float) 12.5, null } },
          { null }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertTrue( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.VARIANT, member.type() );
    assertTrue( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( DataType.STRING, array.get( 0 ).type() );
    assertEquals( DataType.INT32, array.get( 1 ).type() );
    assertEquals( DataType.FLOAT32, array.get( 2 ).type() );
    assertEquals( DataType.NULL, array.get( 3 ).type() );
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testBoleanArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new boolean[] { true, false, true } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.BOOLEAN, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testByteArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new byte[] { 10, 20, 30 } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.INT8, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testShortArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new short[] { 10, 20, 30 } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.INT16, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testIntArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new int[] { 10, 20, 30 } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.INT32, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testlongArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new long[] { 10, 20, 30 } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.INT64, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  
  @Test
  public void testFloatArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new float[] { 10, 20, 30 } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.FLOAT32, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testDoubleArray( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new double[] { 10, 20, 30 } },
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.FLOAT64, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareArrays( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
}
