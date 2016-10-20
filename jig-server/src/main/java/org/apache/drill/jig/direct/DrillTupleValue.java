package org.apache.drill.jig.direct;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.container.FieldValueContainerSet;

public class DrillTupleValue extends AbstractTupleValue {

  private TupleSchemaImpl schema;

  public DrillTupleValue(TupleSchemaImpl schema,
      FieldValueContainerSet containerSet) {
    super( containerSet );
    this.schema = schema;
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

}
