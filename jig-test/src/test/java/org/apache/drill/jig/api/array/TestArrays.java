package org.apache.drill.jig.api.array;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

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

  @Test
  public void testTypedScalarList( ) throws JigException {
    List<Object> list1 = new ArrayList<>( );
    list1.add( "first" );
    list1.add( "second" );
    List<Object> list2 = new ArrayList<>( );
    list2.add( "a" );
    list2.add( "b" );
    list2.add( "c" );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { list1 },
          { list2 }
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
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.getTuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    compareLists( list1, array );
    assertEquals( DataType.STRING, array.get(0).type() );
    assertEquals( list1.get( 0 ), array.get(0).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.getTuple();
    compareLists( list2, tuple.field( 0 ).getArray() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  private void compareLists( List<Object> expected, ArrayValue actual ) {
    assertEquals( expected.size(), actual.size() );
    for ( int i = 0;  i < expected.size( );  i++ ) {
      Object expectedVal = expected.get( i );
      if ( expectedVal == null ) {
        assertTrue( actual.get( i ).isNull() );
      } else {
        assertFalse( actual.get( i ).isNull() );
        assertEquals( expectedVal, actual.get( i ).getValue() );
      }
    }
  }

}
