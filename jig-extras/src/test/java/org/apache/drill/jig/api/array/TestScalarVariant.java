package org.apache.drill.jig.api.array;

import static org.junit.Assert.*;

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

public class TestScalarVariant {

  @Test
  public void testNonNullable() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { true },
          { (byte) 10 },
          { (short) 20 },
          { (int) 30 },
          { (long) 40 },
          { (float) 50 },
          { (double) 60 },
          { new BigDecimal( 70 ) },
          { "80" }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.VARIANT, field.type() );
    assertFalse( field.nullable() );
    
    assertTrue( tupleSet.next( ) );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.BOOLEAN, tuple.field( 0 ).type( ) );
    assertTrue( tuple.field( 0 ).getBoolean() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.INT8, tuple.field( 0 ).type( ) );
    assertEquals( 10, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.INT16, tuple.field( 0 ).type( ) );
    assertEquals( 20, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.INT32, tuple.field( 0 ).type( ) );
    assertEquals( 30, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.INT64, tuple.field( 0 ).type( ) );
    assertEquals( 40, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.FLOAT32, tuple.field( 0 ).type( ) );
    assertEquals( 50, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.FLOAT64, tuple.field( 0 ).type( ) );
    assertEquals( 60, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.DECIMAL, tuple.field( 0 ).type( ) );
    assertEquals( 70, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertEquals( DataType.STRING, tuple.field( 0 ).type( ) );
    assertEquals( 80, tuple.field( 0 ).getInt() );  
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testNullable() throws JigException {
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { true },
          { (int) 30 },
          { "80" },
          { null }
        }
      );
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.VARIANT, field.type() );
    assertTrue( field.nullable() );
    
    assertTrue( tupleSet.next( ) );
    TupleValue tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.BOOLEAN, tuple.field( 0 ).type( ) );
    assertTrue( tuple.field( 0 ).getBoolean() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.INT32, tuple.field( 0 ).type( ) );
    assertEquals( 30, tuple.field( 0 ).getInt() );
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertFalse( tuple.field( 0 ).isNull() );
    assertEquals( DataType.STRING, tuple.field( 0 ).type( ) );
    assertEquals( 80, tuple.field( 0 ).getInt() );  
    
    assertTrue( tupleSet.next( ) );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
    
}
