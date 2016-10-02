package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;

/**
 * Wraps one or more field values for a field, allowing the field value to
 * vary depending on conditions relevant to the field.
 */

public interface FieldValueContainer {
  FieldValue get( );
  
  /**
   * Wraps an array field to allow access to array members.
   * The member is selected with the {@link #bind(int)} method.
   */
  
  public interface IndexableFieldValueContainer extends FieldValueContainer {
    void bind( int index );
  }
}
