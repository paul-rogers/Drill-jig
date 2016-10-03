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
  
  /**
   * Wraps an array field to allow access to array members.
   * The member is selected with the {@link #bind(int)} method.
   */
  
  @Deprecated
  public interface IndexableFieldValueContainer extends FieldValueContainer {
    void bind( IndexedAccessor accessor );
    void bind( int index );
  }
}
