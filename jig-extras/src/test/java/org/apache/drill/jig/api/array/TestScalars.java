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

public class TestScalars {

  @Test
  public void testBoolean( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { true },
          { false }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.BOOLEAN, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.BOOLEAN, tuple.field( 0 ).type() );
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 1, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 1, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 1, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 1, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 1, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 1, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ONE, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "true", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "false", tuple.field( 0 ).getString( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testByte( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (byte) 0 },
          { (byte) 10 }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.INT8, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.INT8, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10", tuple.field( 0 ).getString( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  @Test
  public void testShort( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (short) 0 },
          { (short) 10 },
          { Short.MAX_VALUE }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.INT16, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.INT16, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( Short.MAX_VALUE, tuple.field( 0 ).getShort( ) );
     
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  
  @Test
  public void testInt( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (int) 0 },
          { (int) 10 },
          { Integer.MAX_VALUE }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.INT32, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.INT32, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( Integer.MAX_VALUE, tuple.field( 0 ).getInt( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testLong( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (long) 0 },
          { (long) 10 },
          { Long.MAX_VALUE }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.INT64, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.INT64, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( Long.MAX_VALUE, tuple.field( 0 ).getLong( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testFloat( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (float) 0 },
          { (float) 10 },
          { 22.5f }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.FLOAT32, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.FLOAT32, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0.0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10.0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 22.5f, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( 23, tuple.field( 0 ).getInt( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testDouble( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { (double) 0 },
          { (double) 10 },
          { 22.5d }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.FLOAT64, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.FLOAT64, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0.0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10.0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 22.5d, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( 23, tuple.field( 0 ).getInt( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testDecimal( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new BigDecimal( 0 ) },
          { new BigDecimal( 10 ) }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.DECIMAL, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.DECIMAL, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10", tuple.field( 0 ).getString( ) );
    
   assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testString( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { "0" },
          { "10" },
          { "true" }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.STRING, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.STRING, tuple.field( 0 ).type() );
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 0, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 0, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 0, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 0, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 0, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 0, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( BigDecimal.ZERO, tuple.field( 0 ).getDecimal( ) );
    assertEquals( "0", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).getBoolean() );
    assertEquals( (byte) 10, tuple.field( 0 ).getByte( ) );
    assertEquals( (short) 10, tuple.field( 0 ).getShort( ) );
    assertEquals( (int) 10, tuple.field( 0 ).getInt( ) );
    assertEquals( (long) 10, tuple.field( 0 ).getLong( ) );
    assertEquals( (float) 10, tuple.field( 0 ).getFloat( ), 0.001 );
    assertEquals( (double) 10, tuple.field( 0 ).getDouble( ), 0.001 );
    assertEquals( new BigDecimal( 10 ), tuple.field( 0 ).getDecimal( ) );
    assertEquals( "10", tuple.field( 0 ).getString( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( "true", tuple.field( 0 ).getString( ) );
    assertTrue( tuple.field( 0 ).getBoolean( ) );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testNull( ) throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.NULL, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    assertEquals( DataType.NULL, tuple.field( 0 ).type() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
}
