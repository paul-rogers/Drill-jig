package org.apache.drill.jig.serde;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.array.TestArrayImpl;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.ArrayResultCollection;
import org.apache.drill.jig.serde.deserializer.SimpleBufferResultSet;
import org.apache.drill.jig.serde.serializer.TupleSetSerializer;
import org.junit.Test;

public class SimpleSerdeTest
{

  @Test
  public void testScalar() throws Exception {
    ResultCollection bufferSet = SerdeTestUtils.serdeBatches( TestArrayImpl.makeTypesBatch( ) );
    TestArrayImpl.validateTypesResults(bufferSet );
    bufferSet.close();
  }


}
