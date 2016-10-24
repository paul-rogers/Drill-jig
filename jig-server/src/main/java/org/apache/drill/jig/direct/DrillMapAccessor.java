package org.apache.drill.jig.direct;

import org.apache.drill.jig.accessor.FieldAccessor.TupleValueAccessor;
import org.apache.drill.jig.api.TupleValue;

public class DrillMapAccessor implements TupleValueAccessor {
  
  public DrillTupleValue tuple;
  
  public DrillMapAccessor( DrillTupleValue tuple ) {
    this.tuple = tuple;
  }

  @Override
  public boolean isNull() {
    
    // Drill maps cannot be null. They can have null fields, however.
    
    return false;
  }

  @Override
  public TupleValue getTuple() {
    return tuple;
  }
}
