package org.apache.drill.jig.api.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

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

public class TestNullableScalars {

  @Test
  public void testBoolean( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { true },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.BOOLEAN, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertTrue( tuple.field( 0 ).getBoolean() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testByte( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (byte) 10 },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.INT8, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testShort( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (short) 10 },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.INT16, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  
  @Test
  public void testInt( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (int) 10 },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.INT32, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testLong( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (long) 10 },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.INT64, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testFloat( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (float) 10 },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.FLOAT32, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testDouble( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (double) 10 },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.FLOAT64, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testString( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { "str" },
          { null },
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.STRING, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( "str", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
}
