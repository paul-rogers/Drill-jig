package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.FieldValueCache;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.util.JigUtilities;

/**
 * Field value container for a variant field. The container holds a cache
 * of field values so that the type-specific field values are reused across
 * multiple fields.
 */

public class VariantFieldValueContainer implements FieldValueContainer {

  private final FieldValueCache valueCache;
  private TypeAccessor typeAccessor;

  public VariantFieldValueContainer( FieldValueFactory factory ) {
    valueCache = new FieldValueCache( factory );
  }

  @Override
  public void bind(FieldAccessor accessor) {
    this.typeAccessor = (TypeAccessor) accessor;
  }

  @Override
  public FieldValue get() {
    // Map null fields to the Null type.
    DataType type = (typeAccessor.isNull()) ? DataType.NULL : typeAccessor.getType();
    AbstractFieldValue value = valueCache.get( type );
    value.bind( typeAccessor );
    return value;
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, indent + 1, "type accessor", typeAccessor);
    JigUtilities.visualizeLn(buf, indent + 1, "value cache", valueCache);
    JigUtilities.indent( buf, indent );
    buf.append( "]" );
  }
}
