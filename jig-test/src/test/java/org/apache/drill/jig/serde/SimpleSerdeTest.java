package org.apache.drill.jig.serde;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.array.TestArrayImpl;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.Batch;
import org.junit.Test;

/**
 * Tests that entire tuples are deserialized correctly. Other tests
 * exercise each individual value. This test verifies that field offsets
 * are computed for scalar types and structured types.
 */

public class SimpleSerdeTest
{

  @Test
  public void testScalar() throws Exception {
    ResultCollection bufferSet = SerdeTestUtils.serdeBatches( TestArrayImpl.makeTypesBatch( ) );
    TestArrayImpl.validateTypesResults( bufferSet );
    bufferSet.close();
  }
  
  @Test
  public void testStructured() throws JigException {
    Batch batch = new Batch(
        new String[] { "col1", "col2", "col3", "col4" },
        new Object[][] {
          { 1, SerdeTestUtils.map1( ), new String[] { "a", "b", "c" }, "foo" },
          { 2, SerdeTestUtils.map2( ), new String[] { "x", "y" }, "bar" },
        }
      );

    SerdeTestUtils.validateSerde( batch );
  }

}
