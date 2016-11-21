package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.util.Visualizable;

/**
 * Wraps one or more field values for a field, allowing the field value to
 * vary depending on conditions relevant to the field.
 */

public interface FieldValueContainer extends Visualizable {
  void bind( FieldAccessor accessor );
  FieldValue get( );
}
