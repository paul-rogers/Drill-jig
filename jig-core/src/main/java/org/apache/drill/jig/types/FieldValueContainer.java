package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.FieldAccessor.IndexedAccessor;

/**
 * Wraps one or more field values for a field, allowing the field value to
 * vary depending on conditions relevant to the field.
 */

public interface FieldValueContainer {
  void bind( FieldAccessor accessor );
  FieldValue get( );
}
