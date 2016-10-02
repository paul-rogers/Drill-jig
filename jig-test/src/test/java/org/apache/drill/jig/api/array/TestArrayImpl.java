package org.apache.drill.jig.api.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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

public class TestArrayImpl {

  @Test
  public void testBasics() throws JigException {
    Batch batches[] = {
        new Batch(
            new String[] { "str", "int32", "boolean" },
            new Object[][] {
              { "first", 1, true },
              { "second", 10, false }
            }
        )
    };
    
    ArrayResultCollection results = new ArrayResultCollection( batches );
    assertEquals( -1, results.getIndex() );
    assertTrue( results.next() );
    assertEquals( 0, results.getIndex() );
    TupleSet tupleSet = results.getTuples();
    assertNotNull( tupleSet );
    assertEquals( -1, tupleSet.getIndex() );
    TupleSchema schema = tupleSet.schema();
    assertNotNull( schema );
    assertEquals( 3, schema.count() );
    assertEquals( "str", schema.field( 0 ).name() );
    assertEquals( DataType.STRING, schema.field( 0 ).type() );
    assertTrue( schema.field(0).nullable() );
    assertEquals( "int32", schema.field( 1 ).name() );
    assertEquals( DataType.INT32, schema.field( 1 ).type() );
    assertEquals( "boolean", schema.field( 2 ).name() );
    assertEquals( DataType.BOOLEAN, schema.field( 2 ).type() );
    
    assertTrue( tupleSet.next() );
    assertEquals( 0, tupleSet.getIndex() );
    TupleValue tuple = tupleSet.getTuple();
    assertNotNull( tuple );
    assertEquals( "first", tuple.field( 0 ).getString() );
    assertEquals( 1, tuple.field(1).getInt() );
    assertTrue( tuple.field(2).getBoolean() );
    
    assertTrue( tupleSet.next() );
    assertEquals( 1, tupleSet.getIndex() );
    tuple = tupleSet.getTuple();
    assertNotNull( tuple );
    assertEquals( "second", tuple.field( 0 ).getString() );
    assertEquals( 10, tuple.field(1).getInt() );
    assertFalse( tuple.field(2).getBoolean() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close();
  }
  
  public static Batch[] makeTypesBatch( ) {
    Batch batches[] = {
        new Batch(
            new String[] { "str", "i8", "i16", "i32", "i64", "f", "d", "b", "dec" },
            new Object[][] {
              { "first", (byte) 1, (short) 2, (int) 3, (long) 4, (float) 5.0, (double) 6.0, true, new BigDecimal( 7 ) },
              { "second", null, null, null, null, null, null, null, null },
              { null, (byte) 31, null, (int) 33, null, (float) 35.0, null, false, null },
            }
        )
    };
    return batches;
  }
  
  @Test
  public void testTypes() throws JigException {
    
    ArrayResultCollection results = new ArrayResultCollection( makeTypesBatch( ) );
    validateTypesResults( results );    
    results.close( );
  }
  
  public static void validateTypesResults( ResultCollection results ) throws JigException {
    assertTrue( results.next( ) );
    TupleSet tuples = results.getTuples();
    assertTrue( tuples.next() );
    TupleValue tuple = tuples.getTuple();
    assertEquals( "first", tuple.field( 0 ).getString() );
    assertEquals( 1, tuple.field( 1 ).getByte() );
    assertEquals( 2, tuple.field( 2 ).getShort() );
    assertEquals( 3, tuple.field( 3 ).getInt() );
    assertEquals( 4, tuple.field( 4 ).getLong() );
    assertEquals( 5.0, tuple.field( 5 ).getFloat(), 0.001 );
    assertEquals( 6.0, tuple.field( 6 ).getDouble(), 0.001 );
    assertTrue( tuple.field( 7 ).getBoolean() );
    assertEquals( new BigDecimal( 7 ), tuple.field( 8 ).getDecimal() );

    assertTrue( tuples.next() );
    tuple = tuples.getTuple();
    assertEquals( "second", tuple.field( 0 ).getString() );
    assertTrue( tuple.field( 1 ).isNull( ) );
    assertTrue( tuple.field( 2 ).isNull( ) );
    assertTrue( tuple.field( 3 ).isNull( ) );
    assertTrue( tuple.field( 4 ).isNull( ) );
    assertTrue( tuple.field( 5 ).isNull( ) );
    assertTrue( tuple.field( 6 ).isNull( ) );
    assertTrue( tuple.field( 7 ).isNull( ) );
    assertTrue( tuple.field( 8 ).isNull( ) );

    assertTrue( tuples.next() );
    tuple = tuples.getTuple();
    assertTrue( tuple.field( 0 ).isNull( ) );
    assertEquals( 31, tuple.field( 1 ).getByte() );
    assertTrue( tuple.field( 2 ).isNull( ) );
    assertEquals( 33, tuple.field( 3 ).getInt() );
    assertTrue( tuple.field( 4 ).isNull( ) );
    assertEquals( 35.0, tuple.field( 5 ).getFloat(), 0.001 );
    assertTrue( tuple.field( 6 ).isNull( ) );
    assertFalse( tuple.field( 7 ).getBoolean() );
    assertTrue( tuple.field( 8 ).isNull( ) );
    
    assertFalse( tuples.next() );
    assertFalse( results.next() );
  }
}
