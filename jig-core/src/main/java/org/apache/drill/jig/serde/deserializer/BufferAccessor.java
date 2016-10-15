package org.apache.drill.jig.serde.deserializer;

public class BufferAccessor {

  TupleReader reader;
  
  public void bind( TupleReader reader ) {
    this.reader = reader;
  }
  
  public void seekTo( int posn ) {
    reader.seek( posn );
  }
}
