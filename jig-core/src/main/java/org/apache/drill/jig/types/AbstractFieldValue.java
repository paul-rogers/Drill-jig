package org.apache.drill.jig.types;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.api.impl.InternalFieldValue;

/**
 * Base implementation for a field value that allows binding to
 * a field accessor that provides the required data value.
 */

public interface AbstractFieldValue extends InternalFieldValue {

  public void bind(FieldAccessor accessor);
}
