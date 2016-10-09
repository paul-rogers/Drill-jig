package org.apache.drill.jig.api.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.ArrayResultCollection;
import org.apache.drill.jig.extras.array.Batch;
import org.junit.Test;

public class TestMap {

  @Test
  public void testFixedType() throws JigException {
    Map<String,Integer> map1 = new HashMap<>( );
    map1.put( "one", 1 );
    map1.put( "two", 2 );
    map1.put( "three", 3 );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { map1 }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.MAP, field.type() );
    assertFalse( field.nullable() );
    assertNull( field.member( ) );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    MapValue array = tuple.field( 0 ).getMap();
    Helpers.compareMaps( batch.data[0][0], array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

}
