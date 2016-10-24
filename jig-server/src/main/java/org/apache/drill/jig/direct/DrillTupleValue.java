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
  
  public static class DrillRootTupleValue extends DrillTupleValue {

    public VectorAccessor[] vectorBindings;

    public DrillRootTupleValue(TupleSchemaImpl schema,
        FieldValueContainerSet containerSet,
        VectorAccessor[] vectorBindings) {
      super(schema, containerSet);
      this.vectorBindings = vectorBindings;
    }

    public void bindReader(VectorRecordReader reader) {
      for ( int i = 0;  i < vectorBindings.length;  i++ ) {
        vectorBindings[i].bindReader(reader);
      }
    }
       
    private void bindVectors( ) {
      for ( int i = 0;  i < vectorBindings.length;  i++ ) {
        vectorBindings[i].bindVector( );
      }
    }

    public void start() {
      super.reset();
      bindVectors();
    }
  }

}
