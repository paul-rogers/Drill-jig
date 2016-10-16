package org.apache.drill.jig.serde.deserializer;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.types.FieldValueContainerSet;

public class BufferTupleValue extends AbstractTupleValue {

  private TupleSchema schema;

  public BufferTupleValue(TupleSchema schema, FieldValueContainerSet containers) {
    super(containers);
    this.schema = schema;
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

}
