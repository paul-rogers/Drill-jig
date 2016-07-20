package org.apache.drill.jig.api.array;

import static org.junit.Assert.*;

import java.math.BigDecimal;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleAccessor;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.array.ArrayResultCollection;
import org.apache.drill.jig.api.array.ArrayResultCollection.Batch;
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
    TupleSchema schema = tupleSet.getSchema();
    assertNotNull( schema );
    assertEquals( 3, schema.getCount() );
    assertEquals( "str", schema.getField( 0 ).getName() );
    assertEquals( DataType.STRING, schema.getField( 0 ).getType() );
    assertEquals( Cardinality.OPTIONAL, schema.getField(0).getCardinality() );
    assertEquals( "int32", schema.getField( 1 ).getName() );
    assertEquals( DataType.INT32, schema.getField( 1 ).getType() );
    assertEquals( "boolean", schema.getField( 2 ).getName() );
    assertEquals( DataType.BOOLEAN, schema.getField( 2 ).getType() );
    
    assertTrue( tupleSet.next() );
    assertEquals( 0, tupleSet.getIndex() );
    TupleAccessor tuple = tupleSet.getTuple();
    assertNotNull( tuple );
    assertEquals( "first", tuple.getField( 0 ).asScalar().getString() );
    assertEquals( 1, tuple.getField(1).asScalar().getInt() );
    assertTrue( tuple.getField(2).asScalar().getBoolean() );
    
    assertTrue( tupleSet.next() );
    assertEquals( 1, tupleSet.getIndex() );
    tuple = tupleSet.getTuple();
    assertNotNull( tuple );
    assertEquals( "second", tuple.getField( 0 ).asScalar().getString() );
    assertEquals( 10, tuple.getField(1).asScalar().getInt() );
    assertFalse( tuple.getField(2).asScalar().getBoolean() );
    
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
    TupleAccessor tuple = tuples.getTuple();
    assertEquals( "first", tuple.getField( 0 ).asScalar().getString() );
    assertEquals( 1, tuple.getField( 1 ).asScalar().getByte() );
    assertEquals( 2, tuple.getField( 2 ).asScalar().getShort() );
    assertEquals( 3, tuple.getField( 3 ).asScalar().getInt() );
    assertEquals( 4, tuple.getField( 4 ).asScalar().getLong() );
    assertEquals( 5.0, tuple.getField( 5 ).asScalar().getFloat(), 0.001 );
    assertEquals( 6.0, tuple.getField( 6 ).asScalar().getDouble(), 0.001 );
    assertTrue( tuple.getField( 7 ).asScalar().getBoolean() );
    assertEquals( new BigDecimal( 7 ), tuple.getField( 8 ).asScalar().getDecimal() );

    assertTrue( tuples.next() );
    tuple = tuples.getTuple();
    assertEquals( "second", tuple.getField( 0 ).asScalar().getString() );
    assertTrue( tuple.getField( 1 ).isNull( ) );
    assertTrue( tuple.getField( 2 ).isNull( ) );
    assertTrue( tuple.getField( 3 ).isNull( ) );
    assertTrue( tuple.getField( 4 ).isNull( ) );
    assertTrue( tuple.getField( 5 ).isNull( ) );
    assertTrue( tuple.getField( 6 ).isNull( ) );
    assertTrue( tuple.getField( 7 ).isNull( ) );
    assertTrue( tuple.getField( 8 ).isNull( ) );

    assertTrue( tuples.next() );
    tuple = tuples.getTuple();
    assertTrue( tuple.getField( 0 ).isNull( ) );
    assertEquals( 31, tuple.getField( 1 ).asScalar().getByte() );
    assertTrue( tuple.getField( 2 ).isNull( ) );
    assertEquals( 33, tuple.getField( 3 ).asScalar().getInt() );
    assertTrue( tuple.getField( 4 ).isNull( ) );
    assertEquals( 35.0, tuple.getField( 5 ).asScalar().getFloat(), 0.001 );
    assertTrue( tuple.getField( 6 ).isNull( ) );
    assertFalse( tuple.getField( 7 ).asScalar().getBoolean() );
    assertTrue( tuple.getField( 8 ).isNull( ) );
    
    assertFalse( tuples.next() );
    assertFalse( results.next() );
  }
}
