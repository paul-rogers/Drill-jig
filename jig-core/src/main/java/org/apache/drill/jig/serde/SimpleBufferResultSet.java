package org.apache.drill.jig.serde;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;

public class SimpleBufferResultSet implements ResultCollection
{
  public class SimpleBufferTupleSet implements TupleSet
  {
    boolean isEof = false;
    int tupleIndex = -1;
    
    @Override
    public TupleSchema schema() {
      return deserializer.getSchema( );
    }

    @Override
    public int getIndex() {
      return tupleIndex;
    }

    @Override
    public boolean next() {
      if ( isEof )
        return false;
      if ( tupleIndex > -1 )
        deserializer.endTuple();
      tupleIndex++;
      isEof = ! deserializer.deserializeTuple( buf );
      return ! isEof;
    }

    @Override
    public TupleValue getTuple() {
      return deserializer.getTuple( );
    }
  }

  TupleSetDeserializer deserializer;
  int tupleSetIndex = -1;
  private ByteBuffer buf;
  SimpleBufferTupleSet tupleSet = new SimpleBufferTupleSet( );
  
  public SimpleBufferResultSet( ByteBuffer buf ) {
    deserializer = new TupleSetDeserializer( );
    this.buf = buf;
  }
  
  @Override
  public int getIndex() {
    return tupleSetIndex;
  }

  @Override
  public boolean next() {
    if ( tupleSetIndex > 0 )
      return false;
    tupleSetIndex++;
    if ( tupleSetIndex > 0 )
      return false;
    deserializer.deserializeAndPrepareSchema( buf );
    return true;
  }

  @Override
  public TupleSet getTuples() {
    return tupleSet;
  }

  @Override
  public void close() { }
}
