package org.apache.drill.jig.extras.array;

import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;

public class ArrayFieldHandle implements ObjectAccessor {

  public interface ArrayTupleHandle {
    Object get( int fieldIndex );
  }
  
  private final ArrayTupleHandle tupleHandle;
  private final int fieldIndex;
  
  public ArrayFieldHandle(ArrayTupleHandle tupleHandle, int fieldIndex) {
    this.tupleHandle = tupleHandle;
    this.fieldIndex = fieldIndex;
  }

  @Override
  public boolean isNull() {
    return getObject( ) == null;
  }

  @Override
  public Object getObject() {
    return tupleHandle.get( fieldIndex );
  }

}
