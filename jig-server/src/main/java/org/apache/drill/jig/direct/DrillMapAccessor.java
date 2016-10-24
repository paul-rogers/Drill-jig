package org.apache.drill.jig.direct;

import org.apache.drill.jig.accessor.FieldAccessor.TupleValueAccessor;
import org.apache.drill.jig.api.TupleValue;

/**
 * Accessor for a Map field in Drill. Drill map fields are "virtual" they are
 * not an actual field, but rather a collection of fields. Since the set of
 * fields within the Drill map is predefined, a Drill Map is analogous
 * to a Jig Tuple.
 */

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
