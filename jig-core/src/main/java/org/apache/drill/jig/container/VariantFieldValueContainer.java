package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.FieldValueCache;
import org.apache.drill.jig.types.FieldValueFactory;

/**
 * Field value container for a variant field. The container holds a cache
 * of field values so that the type-specific field values are reused across
 * multiple fields.
 */

public class VariantFieldValueContainer implements FieldValueContainer {

  private final FieldValueCache valueCache;
  private TypeAccessor accessor;

  public VariantFieldValueContainer( FieldValueFactory factory ) {
    valueCache = new FieldValueCache( factory );
  }

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (TypeAccessor) accessor;
  }
  
  @Override
  public FieldValue get() {
    // Map null fields to the Null type.
    DataType type = (accessor.isNull()) ? DataType.NULL : accessor.getType();
    AbstractFieldValue value = valueCache.get( type );
    value.bind( accessor );
    return value;
  }
}
