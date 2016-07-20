package org.apache.drill.jig.serde;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.array.ArrayResultCollection;
import org.apache.drill.jig.api.array.TestArrayImpl;
import org.junit.Test;

public class SimpleSerdeTest
{

  @Test
  public void testScalar() throws Exception {
    ArrayResultCollection results = new ArrayResultCollection( TestArrayImpl.makeTypesBatch( ) );
    assertTrue( results.next( ) );
    TupleSet tuples = results.getTuples( );    

    ByteBuffer buf = ByteBuffer.allocate( 4096 );
    loadBuffer( tuples, buf );
    results.close( );
    
    buf.flip();
    SimpleBufferResultSet bufferSet = new SimpleBufferResultSet( buf );
    TestArrayImpl.validateTypesResults(bufferSet );
    bufferSet.close();
  }

  private void loadBuffer(TupleSet tuples, ByteBuffer buf) throws JigException {
    TupleSetSerializer serializer = new TupleSetSerializer( tuples.getSchema() );
    serializer.serializeSchema( buf );
    while ( tuples.next() ) {
      serializer.serializeTuple( buf, tuples.getTuple() );
    }
  }

}
