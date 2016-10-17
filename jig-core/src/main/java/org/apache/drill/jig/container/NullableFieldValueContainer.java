package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.NullFieldValue;

/**
 * Field value container for a nullable non-variant field.
 */

public class NullableFieldValueContainer implements FieldValueContainer {

  private final AbstractFieldValue nonNullValue;
  private FieldAccessor accessor;

  public NullableFieldValueContainer( AbstractFieldValue nonNullValue ) {
    this.nonNullValue = nonNullValue;
  }

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = accessor;
    nonNullValue.bind( accessor );
  }
  
  @Override
  public FieldValue get() {
    if ( accessor.isNull() )
      return NullFieldValue.INSTANCE;
    else
      return nonNullValue;
  }
}
