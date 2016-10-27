package org.apache.drill.jig.direct;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.container.FieldValueContainerSet;

/**
 * Jig representation of a Drill record. The tuple provides a tuple-like
 * representation of a Drill vector bundle. In Drill, vectors are unordered
 * within a bundle. Jig imposes an order so that field indexes work.
 * <p>
 * This class associates a reader with the accessor, binds accessors to
 * vectors on each new batch, and provides the mapping from field index
 * or name to a Field Value that represents a vector.
 */

public class DirectTupleValue extends AbstractTupleValue {

  private TupleSchemaImpl schema;

  public DirectTupleValue(TupleSchemaImpl schema,
      FieldValueContainerSet containerSet) {
    super( containerSet );
    this.schema = schema;
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }
  
  public static class DrillRootTupleValue extends DirectTupleValue {

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
       
    public void bindVectors( ) {
      for ( int i = 0;  i < vectorBindings.length;  i++ ) {
        vectorBindings[i].bindVector( );
      }
    }

    public void start() {
      super.reset();
    }
  }
}
