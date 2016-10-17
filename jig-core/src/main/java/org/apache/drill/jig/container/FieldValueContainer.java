package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.api.FieldValue;

/**
 * Wraps one or more field values for a field, allowing the field value to
 * vary depending on conditions relevant to the field.
 */

public interface FieldValueContainer {
  void bind( FieldAccessor accessor );
  FieldValue get( );
}
