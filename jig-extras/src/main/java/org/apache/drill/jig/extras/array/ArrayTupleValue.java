package org.apache.drill.jig.extras.array;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.container.FieldValueContainerSet;

public class ArrayTupleValue extends AbstractTupleValue {

  private final ArrayTupleSet tupleSet;

  public ArrayTupleValue(ArrayTupleSet tupleSet, FieldValueContainerSet containers) {
    super( containers );
    this.tupleSet = tupleSet;
  }

  @Override
  public TupleSchema schema() {
    return tupleSet.schema();
  }
}