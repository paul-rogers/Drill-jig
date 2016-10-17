package org.apache.drill.jig.extras.array;

import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;

/**
 * Accesses a field within an array. Uses a tuple handle to point
 * to the current tuple, holds an index of the target field. Together,
 * the allow this handle to pick out the same field in a set of tuples.
 */

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
