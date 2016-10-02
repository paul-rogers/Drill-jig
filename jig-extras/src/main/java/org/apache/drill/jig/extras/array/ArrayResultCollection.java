package org.apache.drill.jig.extras.array;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.types.FieldValueFactory;

public class ArrayResultCollection implements ResultCollection {

  private final Batch[] batches;
  private int index = -1;
  private final FieldValueFactory factory = new FieldValueFactory( );
  private ArrayTupleSet tupleSet;
  
  public ArrayResultCollection( Batch batches[] ) {
    this.batches = batches;
  }
  
  public ArrayResultCollection(Batch batch) {
    this( new Batch[] { batch } );
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public boolean next() throws JigException {
    if ( index + 1 < batches.length ) {
      index++;
      tupleSet = new ArrayTupleSet( factory, batches[index] );
      return true;
    }
    tupleSet = null;
    return false;
  }

  @Override
  public TupleSet getTuples() {
    return tupleSet;
  }

  @Override
  public void close() {
  }
}