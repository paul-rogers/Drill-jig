package org.apache.drill.jig.serde.deserializer;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.types.FieldValueContainerSet;

public class BufferTupleValue extends AbstractTupleValue {

  public BufferTupleValue(FieldValueContainerSet containers) {
    super(containers);
  }

  @Override
  public TupleSchema schema() {
    // TODO Auto-generated method stub
    return null;
  }

}
