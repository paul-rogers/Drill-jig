package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.FieldAccessor.IndexedAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.types.FieldValueContainer.IndexableFieldValueContainer;

/**
 * Field value container for an array with variable type elements. This container
 * handles both indexing and selecting field values based on types. The accessor
 * provided must implement {@link TypeAccessor} and all the scalar accessors
 * (or at least as many as are needed for the range of data types allowed for
 * the field.)
 */

public class VariantIndexedFieldValueContainer implements IndexableFieldValueContainer {

  private final IndexedAccessor indexAccessor;
  private final TypeAccessor typeAccessor;
  private final FieldValueCache valueCache;

  public VariantIndexedFieldValueContainer( IndexedAccessor accessor, FieldValueFactory factory ) {
    this.indexAccessor = accessor;
    this.typeAccessor = (TypeAccessor) accessor;
    valueCache = new FieldValueCache( factory );
  }
  
  @Override
  public FieldValue get() {
    if ( indexAccessor.isNull() )
      return NullFieldValue.INSTANCE;
    AbstractFieldValue value = valueCache.get( typeAccessor.getType( ) );
    value.bind( indexAccessor );
    return value;
  }

  @Override
  public void bind(int index) {
    indexAccessor.bind( index );
  }
}
