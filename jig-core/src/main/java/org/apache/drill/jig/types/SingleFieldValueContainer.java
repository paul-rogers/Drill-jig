package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;

/**
 * Field value container for a single field value. Used for non-null, non-variant
 * fields.
 */

public class SingleFieldValueContainer implements FieldValueContainer {

  private final FieldValue value;
  
  public SingleFieldValueContainer( FieldValue value ) {
    this.value = value;
  }
  
  @Override
  public FieldValue get() {
    return value;
  }

}
