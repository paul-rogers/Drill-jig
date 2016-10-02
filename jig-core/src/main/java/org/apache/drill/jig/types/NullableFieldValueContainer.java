package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;

/**
 * Field value container for a nullable non-variant field.
 */

public class NullableFieldValueContainer implements FieldValueContainer {

  private final FieldAccessor accessor;
  private final FieldValue nonNullValue;

  public NullableFieldValueContainer( FieldAccessor accessor, FieldValue nonNullValue ) {
    this.accessor = accessor;
    this.nonNullValue = nonNullValue;
  }
  
  @Override
  public FieldValue get() {
    if ( accessor.isNull() )
      return NullFieldValue.INSTANCE;
    else
      return nonNullValue;
  }

}
