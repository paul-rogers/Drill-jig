package org.apache.drill.jig.extras.array;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;

public class ArrayTupleValue implements TupleValue {

  private ArrayTupleSet tupleSet;
  private FieldValue[] values;

  public ArrayTupleValue(ArrayTupleSet tupleSet, FieldValue[] fieldValues) {
    this.tupleSet = tupleSet;
    this.values = fieldValues;
  }

  @Override
  public TupleSchema schema() {
    return tupleSet.schema();
  }

  @Override
  public FieldValue field(int i) {
    if (i < 0 || i >= values.length)
      return null;
    return values[i];
  }

  @Override
  public FieldValue field(String name) {
    FieldSchema field = schema().field(name);
    if (field == null)
      return null;
    return field(field.index());
  }
}