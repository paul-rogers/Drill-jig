package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.AbstractFieldValue;

/**
 * Field value container for a single field value. Used for non-null, non-variant
 * fields.
 */

public class SingleFieldValueContainer implements FieldValueContainer {

  private final AbstractFieldValue value;
  
  public SingleFieldValueContainer( AbstractFieldValue value ) {
    this.value = value;
  }

  @Override
  public void bind(FieldAccessor accessor) {
    value.bind( accessor );
  }
  
  @Override
  public FieldValue get() {
    return value;
  }

}
