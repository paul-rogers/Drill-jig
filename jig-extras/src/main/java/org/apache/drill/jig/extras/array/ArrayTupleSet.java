package org.apache.drill.jig.extras.array;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.ArrayFieldHandle.ArrayTupleHandle;
import org.apache.drill.jig.types.FieldValueFactory;

public class ArrayTupleSet implements TupleSet, ArrayTupleHandle {

  private final Batch batch;
  private final TupleSchema schema;
  private final ArrayTupleValue tuple;
  private int rowIndex = -1;
  
  public ArrayTupleSet(FieldValueFactory factory, Batch batch) {
    this.batch = batch;
    SchemaBuilder builder = new SchemaBuilder( factory );
    schema = builder.buildSchema( batch );
    tuple = new ArrayTupleValue( this, builder.fieldValues( this ) );
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

  @Override
  public int getIndex() {
    return rowIndex;
  }

  @Override
  public boolean next() throws JigException {
    if ( rowIndex + 1 < batch.data.length ) {
      rowIndex++;
      return true;
    }
    return false;
  }

  @Override
  public TupleValue tuple() {
    if ( rowIndex < batch.data.length )
     return tuple;
   else
     return null;
  }

  @Override
  public Object get(int fieldIndex) {
    return batch.data[ rowIndex ][ fieldIndex ];
  } 
}