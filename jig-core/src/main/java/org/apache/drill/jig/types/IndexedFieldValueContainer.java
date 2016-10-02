package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.FieldAccessor.IndexedAccessor;
import org.apache.drill.jig.types.FieldValueContainer.IndexableFieldValueContainer;

/**
 * Field value container for a non-variant, possibly nullable array member.
 */

public class IndexedFieldValueContainer implements IndexableFieldValueContainer {

  private IndexedAccessor accessor;
  private final FieldValue nonNullValue;

  public IndexedFieldValueContainer( FieldValue nonNullValue ) {
    this.nonNullValue = nonNullValue;
  }

  @Override
  public void bind(IndexedAccessor accessor) {
    this.accessor = accessor;
  }
  
  @Override
  public FieldValue get() {
    if ( accessor.isNull() )
      return NullFieldValue.INSTANCE;
    return nonNullValue;
  }

  @Override
  public void bind(int index) {
    accessor.bind( index );
  }

}
