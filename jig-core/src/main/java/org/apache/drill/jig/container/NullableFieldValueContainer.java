package org.apache.drill.jig.container;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.NullFieldValue;
import org.apache.drill.jig.util.JigUtilities;

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

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, indent + 1, "value", nonNullValue);
    JigUtilities.visualizeLn(buf, indent + 1, "accessor", accessor);
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "]" );
  }
}
