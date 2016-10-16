package org.apache.drill.jig.serde.deserializer;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;

public class SimpleBufferResultSet implements ResultCollection
{
  public static class SimpleBufferTupleSet implements TupleSet
  {
    private boolean isEof = false;
    private int tupleIndex = -1;
    private final ByteBuffer buf;
    private final TupleSetDeserializer deserializer;
    private final AbstractTupleValue tuple;
    
    public SimpleBufferTupleSet( TupleSchema schema, ByteBuffer buf ) {
      this.buf = buf;
      deserializer = new TupleSetDeserializer( schema );
      TupleBuilder builder = new TupleBuilder( deserializer );
      tuple = builder.build( schema );
    }
    
    @Override
    public TupleSchema schema() {
      return tuple.schema( );
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
      tuple.reset();
      isEof = ! deserializer.startTuple( buf );
      return ! isEof;
    }

    @Override
    public TupleValue tuple() {
      return tuple;
    }
  }

  private int tupleSetIndex = -1;
  private final SimpleBufferTupleSet tupleSet;
  
  public SimpleBufferResultSet( TupleSchema schema, ByteBuffer buf ) {
    tupleSet = new SimpleBufferTupleSet( schema, buf );
  }
  
  @Override
  public int index() {
    return tupleSetIndex;
  }

  @Override
  public boolean next() {
    if ( tupleSetIndex > 0 )
      return false;
    tupleSetIndex++;
    if ( tupleSetIndex > 0 )
      return false;
    return true;
  }

  @Override
  public TupleSet tuples() {
    return tupleSet;
  }

  @Override
  public void close() { }
}
